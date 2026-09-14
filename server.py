#!/usr/bin/env python3
"""
MultiScreen — local server.

Serves the static app (index.html) and exposes a small API:

  GET /api/resolve?url=<page>[&q=<height>][&scan=1]
      Uses yt-dlp to find the real stream on ~1800 video sites.
      Returns JSON: { ok, title, stream, isHls } where `stream` is a URL
      that already goes through our /api/proxy (with the right headers).
      Results are cached for a few minutes so reloads are instant.

      With scan=1 the page is ALSO scanned for <video> elements, in parallel
      with the extraction — a page hosting several videos comes back as
      { ok, title, videos: [...] } (one tile each), and the scan doubles as
      the fallback when yt-dlp can't extract anything. Running the two
      concurrently is deliberate: done one after the other, slow pages used
      to hold every tile's extraction hostage behind a page download.

  GET /api/scan?url=<page>
      The page scan on its own: one proxied entry per <video> element found
      (Referer set to the page). Cached, like resolves.

  GET /api/wrap?url=<media>[&ref=<page>][&title=<t>]
      Turns a media URL the browser already found (the companion extension
      sniffs the real .m3u8/.mpd behind a blob: player) into a proxied,
      CORS-enabled entry. Builds a proxy token; downloads nothing.

  GET /api/voices[?lang=pt-BR][&gender=female]
      The neural voice roster for Trance (edge-tts, free, no key). Gender
      comes from the service, so "female" is a fact here, not a guess from
      the first name. 503 when edge-tts isn't installed — the page then
      falls back to the browser's own voices.

  GET /api/tts?text=<phrase>&voice=<ShortName>[&rate=-25%][&pitch=-10Hz]
      One phrase as MP3, memoised — a looping mantra only hits the network
      the first time it is spoken.

  POST /api/analyze   {url[, quality, duration, fresh, probe]}
      Measures one video with ffmpeg — scene cuts, motion and loudness — and
      returns one value per second, which is what the Moments panel marks the
      good parts from. No model, nothing paid: a single 4 fps pass, cached on
      disk. `probe` only answers whether the curve already exists, so opening
      the panel on a 40-tile wall doesn't start 40 decodes.
      Progress: GET /api/analyze/status?id=, curves: /api/analyze/result?id=.

  POST /api/clip/index  {url, model}  +  POST /api/clip/search {url, prompts}
      Zero-shot search inside a video, with OpenAI's CLIP running locally
      through onnxruntime: one frame a second becomes a vector, your phrase
      becomes one too, and a softmax against a dozen ordinary phrases turns
      the pair into "how much of this second is what you asked for", 0..1 —
      an absolute scale, so a phrase that isn't in the video reads as zero
      instead of as the least bad second. Three model sizes (B/32, B/16,
      L/14), each fetched once by POST /api/clip/setup; GET /api/clip/status
      says what is ready. Optional: without numpy/onnxruntime the Moments
      panel simply loses the text box and keeps working on scene cuts,
      motion and loudness.

  POST /api/channel/list      {url[, limit]}
  POST /api/channel/compile   {url[, limit, preset, model, resolution]}
      A performer or channel page in, one compilation out, with no browser in
      the loop: yt-dlp lists the videos, each is indexed and scored against the
      preset, the clip is picked server-side and ffmpeg joins them. Progress at
      GET /api/channel/status?id=, and the file at /api/compile/result?id=
      using the `compile_id` the status reports.

  GET /api/proxy?p=<base64>
      Pipes the video/manifest through the server, injecting the headers
      (Referer/User-Agent) the site requires and enabling CORS for the
      browser. For HLS playlists (.m3u8), internal URIs are rewritten to
      also go through the proxy — so hls.js can play them.

Performance: browsers only open ~6 simultaneous connections per host:port.
With 12 videos all streaming through one port, half of them starve. So the
server also listens on a few extra ports and spreads proxied streams across
them round-robin.

Optional extras:
    pip install imageio-ffmpeg  # ffmpeg for Compile/Moments, if not on PATH
    pip install edge-tts        # neural voices for Trance
    pip install numpy onnxruntime          # search moments by describing them
    pip install onnxruntime-directml       # ...on the GPU instead (Windows)

Usage:
    python server.py            # port 8000 (+ proxy shards on 8001-8005)
    python server.py 8080       # custom port
"""

import base64
import collections
import glob
import hashlib
import itertools
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import re
import tempfile
import threading
import time
import uuid
import urllib.parse
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

# How many extra ports to open besides the main one (proxy shards).
# 12 HLS players fetching playlist + segment in bursts overflow the 24
# lanes of 4 ports; 6 ports give the browser 36 concurrent connections.
EXTRA_PROXY_PORTS = 5

# Filled in main(): every port the server listens on (main + shards).
PROXY_PORTS = []
_port_cycle = None
_port_lock = threading.Lock()

# Cache for /api/resolve: (url, qmax) -> (expires_at, payload).
# Stream URLs from yt-dlp usually stay valid for a while; caching skips a
# multi-second extraction on reloads and duplicate tiles.
RESOLVE_TTL = 600  # seconds
_resolve_cache = {}
_resolve_lock = threading.Lock()

# Page scanning runs alongside the yt-dlp extraction rather than before it.
# Done serially, a slow page fetch (up to two attempts, plain then
# impersonated) delayed *every* tile before its extraction even started, and
# with a handful of concurrent slots the whole wall queued up behind a few
# slow pages. Overlapping them makes a tile cost max(scan, resolve).
_SCAN_POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix="scan")
SCAN_BUDGET = 12  # seconds a scan may delay a resolve before we ignore it
SCAN_TTL = 300
_scan_cache = {}
_scan_lock = threading.Lock()

# MIME types by extension for static files.
MIME = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
}


# Upstream connection pooling. urllib has no keep-alive, so every HLS
# segment paid a fresh TCP+TLS handshake (100-300 ms) to the CDN — with 12
# players that latency dominates. yt-dlp already depends on `requests`, so
# use a pooled Session when available and fall back to urllib otherwise.
try:
    import requests as _requests
    from requests.adapters import HTTPAdapter as _HTTPAdapter
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

# A requests.Session is NOT thread-safe (its cookie jar is shared mutable
# state), and this server runs a thread per connection. One shared Session
# across 40 tiles streaming at once is a race waiting to happen — it shows up
# as random, unreproducible upstream failures. So sessions are checked out and
# returned like the curl ones: one owner at a time, still pooled for keep-alive.
_session_pool = []
_session_pool_lock = threading.Lock()
SESSION_POOL_MAX = 16


def _acquire_session():
    with _session_pool_lock:
        if _session_pool:
            return _session_pool.pop()
    s = _requests.Session()
    s.trust_env = False  # no surprise proxies from env vars
    for scheme in ("http://", "https://"):
        s.mount(scheme, _HTTPAdapter(pool_connections=8, pool_maxsize=32))
    return s


def _release_session(sess):
    with _session_pool_lock:
        if len(_session_pool) < SESSION_POOL_MAX:
            _session_pool.append(sess)
            return
    try:
        sess.close()
    except Exception:  # noqa: BLE001
        pass


CHUNK_SIZE = 262144

# In-memory cache of proxied media bodies (HLS segments, small files).
#
# A wall of 40 looping tiles re-fetches the same segments from the CDN on every
# lap. That is what makes the picture stutter: dozens of TLS round-trips per
# second through a GIL-bound proxy, and signed CDNs start rate-limiting (403)
# long before the bandwidth runs out. Segments are small and immutable, so
# keeping them in RAM makes the second lap onward cost nothing and takes the
# CDN out of the loop entirely.
MEDIA_CACHE_MAX_BYTES = 512 * 1024 * 1024
MEDIA_CACHE_MAX_ITEM = 12 * 1024 * 1024
_media_cache = collections.OrderedDict()  # url -> (content_type, body)
_media_cache_bytes = 0
_media_cache_lock = threading.Lock()


def media_cache_get(url):
    with _media_cache_lock:
        hit = _media_cache.get(url)
        if hit is not None:
            _media_cache.move_to_end(url)  # LRU
        return hit


def media_cache_put(url, ctype, body):
    global _media_cache_bytes
    if not body or len(body) > MEDIA_CACHE_MAX_ITEM:
        return
    with _media_cache_lock:
        if url in _media_cache:
            return
        _media_cache[url] = (ctype, body)
        _media_cache_bytes += len(body)
        while _media_cache_bytes > MEDIA_CACHE_MAX_BYTES and _media_cache:
            _, (_, dropped) = _media_cache.popitem(last=False)  # evict oldest
            _media_cache_bytes -= len(dropped)


def _combined_ca_bundle():
    """certifi's roots plus the local Windows cert store, written to a temp PEM.

    On Windows, Python's own ssl picks up the machine's ROOT store (which is
    how the normal client works behind antivirus/corporate TLS interception),
    but curl_cffi/libcurl only trusts the certifi bundle and then fails with
    'unable to get local issuer certificate'. Merging the Windows roots in
    lets impersonation work on those machines too. Returns a path, or None if
    we can't build one (non-Windows or no extra certs — certifi alone is fine).
    """
    try:
        import ssl
        import certifi
        if not hasattr(ssl, "enum_certificates"):
            return None  # not Windows; certifi alone works
        with open(certifi.where(), "r", encoding="utf-8") as f:
            pem = f.read()
        extra = 0
        for store in ("ROOT", "CA"):
            try:
                for cert, enc, _trust in ssl.enum_certificates(store):
                    if enc == "x509_asn":
                        b64 = base64.encodebytes(cert).decode("ascii").strip()
                        pem += ("\n-----BEGIN CERTIFICATE-----\n" + b64 +
                                "\n-----END CERTIFICATE-----\n")
                        extra += 1
            except Exception:  # noqa: BLE001
                pass
        if not extra:
            return None
        import tempfile
        path = os.path.join(tempfile.gettempdir(), "multiscreen_cabundle.pem")
        with open(path, "w", encoding="utf-8") as f:
            f.write(pem)
        return path
    except Exception:  # noqa: BLE001
        return None


def _setup_impersonation():
    """Enable browser impersonation if curl_cffi is available.

    Many sites (Cloudflare-fronted, live TV, file hosts) block yt-dlp by its
    TLS/HTTP fingerprint and answer 403/410 even with a fresh yt-dlp. With
    curl_cffi installed, yt-dlp can mimic a real browser handshake and get
    through. Returns an ImpersonateTarget, or None to keep the normal client.

    Impersonation is a FALLBACK, not the default: YouTube binds the stream
    URLs it hands to an impersonated Chrome to that client session, so the
    proxy's later fetch of those URLs gets 403 — every tile then shows the
    generic playback error. Extracting with the plain client first keeps
    YouTube & friends working; impersonation kicks in only when the plain
    extraction fails (fingerprint-blocking sites).
    """
    import importlib.util
    if importlib.util.find_spec("curl_cffi") is None:
        return None
    # curl_cffi resolves its CA bundle from these env vars *at import time*, so
    # set them before importing it.
    bundle = _combined_ca_bundle()
    if bundle:
        os.environ.setdefault("SSL_CERT_FILE", bundle)
        os.environ.setdefault("CURL_CA_BUNDLE", bundle)
    try:
        import curl_cffi  # noqa: F401
        from yt_dlp.networking.impersonate import ImpersonateTarget
        return ImpersonateTarget.from_str("chrome")
    except Exception:  # noqa: BLE001 — any failure just disables impersonation
        return None


_IMPERSONATE = _setup_impersonation()


class _RequestsUpstream:
    """Response wrapper: pooled keep-alive connection via requests.

    Owns its Session for the life of the response and returns it on close().
    """

    def __init__(self, resp, session=None):
        self._resp = resp
        self._session = session
        self.status = resp.status_code

    def header(self, name, default=None):
        return self._resp.headers.get(name, default)

    def read(self):
        return self._resp.content

    def chunks(self):
        return self._resp.iter_content(CHUNK_SIZE)

    def close(self):
        try:
            self._resp.close()
        except Exception:  # noqa: BLE001
            pass
        sess, self._session = self._session, None  # release exactly once
        if sess is not None:
            _release_session(sess)


class _UrllibUpstream:
    """Fallback wrapper: one connection per request (requests not installed)."""

    def __init__(self, resp):
        self._resp = resp
        self.status = getattr(resp, "status", 200)

    def header(self, name, default=None):
        return self._resp.headers.get(name, default)

    def read(self):
        return self._resp.read()

    def chunks(self):
        while True:
            chunk = self._resp.read(CHUNK_SIZE)
            if not chunk:
                break
            yield chunk

    def close(self):
        try:
            self._resp.close()
        except Exception:  # noqa: BLE001
            pass


class _CurlUpstream:
    """Response wrapper: curl_cffi with browser impersonation (streamed).

    Owns a pooled session for the lifetime of the response and hands it back
    on close(), so impersonated streams keep their connection reuse without
    leaking a libcurl handle per request.
    """

    def __init__(self, resp, session=None):
        self._resp = resp
        self._session = session
        self.status = resp.status_code

    def header(self, name, default=None):
        return self._resp.headers.get(name, default)

    def read(self):
        return b"".join(self.chunks())

    def chunks(self):
        return self._resp.iter_content(CHUNK_SIZE)

    def close(self):
        try:
            self._resp.close()
        except Exception:  # noqa: BLE001
            pass
        sess, self._session = self._session, None  # release exactly once
        if sess is not None:
            _release_curl_session(sess)


# curl_cffi sessions are not thread-safe, and ThreadingHTTPServer makes a new
# thread per connection — a thread-local session would build (and leak) one
# libcurl handle per connection over a long wall session. A checked-out pool
# keeps keep-alive reuse with a bounded number of handles: a session is owned
# by exactly one response and goes back when that response closes.
_curl_pool = []
_curl_pool_lock = threading.Lock()
CURL_POOL_MAX = 8


def _acquire_curl_session():
    with _curl_pool_lock:
        if _curl_pool:
            return _curl_pool.pop()
    from curl_cffi import requests as curl_requests
    return curl_requests.Session(impersonate="chrome")


def _release_curl_session(sess):
    with _curl_pool_lock:
        if len(_curl_pool) < CURL_POOL_MAX:
            _curl_pool.append(sess)
            return
    try:
        sess.close()
    except Exception:  # noqa: BLE001
        pass


def _open_upstream_curl(target, headers):
    sess = _acquire_curl_session()
    try:
        resp = sess.get(target, headers=headers, stream=True,
                        timeout=30, allow_redirects=True)
    except Exception:
        _release_curl_session(sess)
        raise
    if resp.status_code >= 400:
        code = resp.status_code
        try:
            resp.close()
        except Exception:  # noqa: BLE001
            pass
        _release_curl_session(sess)
        raise urllib.error.HTTPError(target, code, "upstream error", None, None)
    return _CurlUpstream(resp, sess)


# Upstream statuses worth another try: the CDN is busy/rate-limiting rather
# than refusing us. 403/404/410 mean the URL itself is dead — those go to the
# re-resolve path instead, which is handled by the caller.
_TRANSIENT_STATUS = (408, 425, 429, 500, 502, 503, 504)
UPSTREAM_ATTEMPTS = 3


def _open_upstream_once(target, headers, impersonated):
    if impersonated and _IMPERSONATE is not None:
        return _open_upstream_curl(target, headers)
    try:
        if _HAS_REQUESTS:
            sess = _acquire_session()
            try:
                resp = sess.get(target, headers=headers, stream=True,
                                timeout=(10, 30), allow_redirects=True)
            except Exception:
                _release_session(sess)
                raise
            if resp.status_code >= 400:
                code = resp.status_code
                resp.close()
                _release_session(sess)
                raise urllib.error.HTTPError(target, code, "upstream error", None, None)
            return _RequestsUpstream(resp, sess)
        req = urllib.request.Request(target, headers=headers)
        return _UrllibUpstream(urllib.request.urlopen(req, timeout=30))
    except urllib.error.HTTPError as e:
        if e.code == 403 and _IMPERSONATE is not None:
            return _open_upstream_curl(target, headers)
        raise


def open_upstream(target, headers, impersonated=False, attempts=UPSTREAM_ATTEMPTS):
    """GETs `target`, reusing pooled keep-alive connections when possible.

    `impersonated` marks streams whose extraction needed browser
    impersonation — their CDNs fingerprint clients too, so fetch them with
    curl_cffi from the start. Plain fetches that bounce with 403 also get one
    impersonated retry (hotlink protection that only bites at download time).

    Transient failures (timeouts, dropped connections, 429/5xx) are retried
    with a short backoff. A wall of a dozen tiles hits CDNs in bursts and a
    single hiccup used to kill the tile outright; retrying here is what turns
    those into a barely-noticeable pause.
    """
    last = None
    for i in range(max(1, attempts)):
        try:
            return _open_upstream_once(target, headers, impersonated)
        except urllib.error.HTTPError as e:
            if e.code not in _TRANSIENT_STATUS or i + 1 >= attempts:
                raise
            last = e
        except Exception as e:  # noqa: BLE001 — network errors are all retryable
            if i + 1 >= attempts:
                raise
            last = e
        time.sleep(0.4 * (i + 1))
    raise last


def next_proxy_port():
    """Round-robin over the listening ports to spread browser connections."""
    with _port_lock:
        return next(_port_cycle)


def encode_target(url, headers, impersonated=False, org=None, qmax=None,
                  sub=False, pq=""):
    """Packs the proxy token: stream URL + headers, plus the impersonation
    flag and — for resolved streams — the ORIGINAL page URL and quality, so
    the proxy can re-extract on the spot when the stream URL goes stale
    (expired link, rolling 403 enforcement).

    `sub` marks URLs lifted out of an HLS manifest (variant playlists, key and
    segment URIs). They expire with the same signature as their master, but a
    fresh extraction returns the MASTER — serving that in answer to a segment
    request would hand hls.js the wrong thing. So sub-resources are healed by
    re-signing instead (see `retoken`)."""
    obj = {"url": url, "headers": headers}
    if impersonated:
        obj["imp"] = True
    if org:
        obj["org"] = org
        obj["q"] = qmax
    if sub:
        obj["sub"] = True
    if pq:
        obj["pq"] = pq  # parent playlist's query, for signed-CDN retries
    raw = json.dumps(obj).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


# Query keys that mean "this URL is signed" on the usual CDNs (phncdn,
# CloudFront, Akamai, S3 presigned…). Used to decide whether a playlist's query
# should be carried onto its segments.
_SIGNATURE_KEYS = ("hash", "validto", "validfrom", "expires", "signature",
                   "sig", "token", "policy", "key-pair-id", "st", "e",
                   "x-amz-signature", "ipa")


def _looks_signed(query):
    if not query:
        return False
    keys = {k.lower() for k, _ in urllib.parse.parse_qsl(query, keep_blank_values=True)}
    return any(k in keys for k in _SIGNATURE_KEYS)


def retoken(stale_url, fresh_url):
    """Re-signs `stale_url` with the credentials of a freshly extracted URL.

    Token-signed CDNs (phncdn, CloudFront, Akamai…) hand out links whose query
    string carries the expiry and signature for a whole path prefix, e.g.
    ?validfrom=…&validto=…&hash=… . When those expire, every segment of an
    open wall starts answering 403. Re-extracting the page yields a fresh
    signature; moving it onto the segment we actually want revives it without
    re-reading the manifest. Returns None when there's nothing to transplant.
    """
    fresh_q = urllib.parse.urlsplit(fresh_url).query
    stale = urllib.parse.urlsplit(stale_url)
    if not fresh_q or fresh_q == stale.query:
        return None
    return urllib.parse.urlunsplit(
        (stale.scheme, stale.netloc, stale.path, fresh_q, stale.fragment))


def decode_target(token):
    raw = base64.urlsafe_b64decode(token.encode("ascii"))
    return json.loads(raw.decode("utf-8"))


def is_hls(url, protocol=""):
    return "m3u8" in (protocol or "") or ".m3u8" in url.split("?")[0].lower()


# ---------- ffmpeg (used by /api/compile) ----------

def _find_ffmpeg():
    """Locate an ffmpeg binary: PATH first, then the imageio-ffmpeg bundle.

    `pip install imageio-ffmpeg` ships a static ffmpeg with libx264 + hardware
    encoders (nvenc/qsv/amf), so the compile feature works without a separate
    system install. Returns a path or None.
    """
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:  # noqa: BLE001
        return None


FFMPEG = _find_ffmpeg()


def _detect_h264_encoder():
    """Pick the fastest available H.264 encoder.

    Hardware encoders (NVENC/QSV/AMF) offload compilation from the CPU and are
    much faster; libx264 is the universal fallback. We only probe the list here
    — if a hardware encoder is listed but has no working GPU at runtime, the
    per-clip extraction falls back to libx264 automatically.
    """
    if not FFMPEG:
        return "libx264"
    try:
        out = subprocess.run(
            [FFMPEG, "-hide_banner", "-encoders"],
            capture_output=True, text=True, timeout=20).stdout
    except Exception:  # noqa: BLE001
        return "libx264"
    for enc in ("h264_nvenc", "h264_qsv", "h264_amf"):
        if enc in out:
            return enc
    return "libx264"


VIDEO_ENCODER = _detect_h264_encoder()


def _ytdlp_ffmpeg_dir():
    """A directory containing an 'ffmpeg(.exe)' basename, for yt-dlp.

    yt-dlp locates ffmpeg by standard basename inside `ffmpeg_location`. The
    imageio-ffmpeg binary has a non-standard name (ffmpeg-win-x86_64-*.exe), so
    we expose a correctly-named hardlink (or copy) in a small cache dir. Needed
    so yt-dlp can trim sections and merge DASH audio+video.
    """
    if not FFMPEG:
        return None
    d, base = os.path.split(FFMPEG)
    if base.lower() in ("ffmpeg", "ffmpeg.exe"):
        return d
    name = "ffmpeg.exe" if os.name == "nt" else "ffmpeg"
    cache = os.path.join(tempfile.gettempdir(), "multiscreen_ffmpeg")
    try:
        os.makedirs(cache, exist_ok=True)
        dst = os.path.join(cache, name)
        if not os.path.exists(dst):
            try:
                os.link(FFMPEG, dst)   # instant, no extra disk on the same drive
            except OSError:
                shutil.copy2(FFMPEG, dst)
        return cache
    except Exception:  # noqa: BLE001
        return None


FFMPEG_DIR = _ytdlp_ffmpeg_dir()

# yt-dlp decides whether a partial download is possible via FFmpegFD.available(),
# a static PATH check that ignores `ffmpeg_location`. Put our ffmpeg dir on PATH
# so section downloads (download_ranges) work with the bundled binary.
if FFMPEG_DIR:
    os.environ["PATH"] = FFMPEG_DIR + os.pathsep + os.environ.get("PATH", "")

# Extensions we can hand straight to ffmpeg without a yt-dlp round-trip.
_DIRECT_MEDIA_RE = re.compile(
    r"\.(mp4|webm|ogg|ogv|mov|m4v|mkv|m3u8|ts|mp3|m4a|aac|wav|flac)(\?|$)", re.I)

# Output presets for the compilation (width, height).
COMPILE_RES = {"480": (854, 480), "720": (1280, 720), "1080": (1920, 1080)}

# How many clips to extract at once. Each is a full ffmpeg process; a small
# pool keeps the machine responsive while still overlapping network waits.
COMPILE_WORKERS = min(4, max(2, (os.cpu_count() or 4) - 1))

# Async compile jobs: id -> progress dict. The browser starts a job, polls its
# status (so it can show a real progress bar), then downloads the result.
_compile_jobs = {}
_compile_jobs_lock = threading.Lock()
COMPILE_JOB_TTL = 1800  # forget finished jobs after 30 min

# Uploaded local videos: id -> file path. They're served (with Range support)
# so the browser can play/seek them and ffmpeg can cut them like any URL.
_UPLOAD_DIR = os.path.join(tempfile.gettempdir(), "multiscreen_uploads")
_uploads = {}
_uploads_lock = threading.Lock()


_FID_RE = re.compile(r"^[0-9a-f]{32}$")


def _recover_upload(fid):
    """Locate an uploaded file on disk when the in-memory registry has no entry.

    That registry dies with the process, so restarting the server used to break
    every tile of a restored package even though the files were still sitting in
    the upload directory. The id IS the file's name, so the mapping rebuilds
    itself on demand — which is what makes restarting (to pick up new server
    code, say) cost nothing but the reconnect. The 32-hex check is also what
    keeps this from being a path-traversal hole."""
    if not _FID_RE.match((fid or "").lower()):
        return None
    try:
        for name in os.listdir(_UPLOAD_DIR):
            if os.path.splitext(name)[0].lower() == fid.lower():
                found = os.path.join(_UPLOAD_DIR, name)
                if os.path.isfile(found):
                    with _uploads_lock:
                        _uploads[fid] = found
                    return found
    except OSError:
        pass
    return None


def _venc_args(encoder):
    """ffmpeg video-encoder args for `encoder`, tuned for speed."""
    if encoder == "h264_nvenc":
        return ["-c:v", "h264_nvenc", "-preset", "p4", "-rc", "vbr", "-cq", "23", "-b:v", "0"]
    if encoder == "h264_qsv":
        return ["-c:v", "h264_qsv", "-global_quality", "23"]
    if encoder == "h264_amf":
        return ["-c:v", "h264_amf", "-rc", "cqp", "-qp_i", "23", "-qp_p", "23"]
    return ["-c:v", "libx264", "-preset", "veryfast", "-crf", "21"]


def build_format(qmax):
    """Builds the yt-dlp format string, capping height at `qmax` (px).

    With several videos on the same screen we prefer a single muxed file
    (audio+video) up to `qmax` tall, H.264 (hardware decoding on any GPU,
    unlike VP9/AV1 on older machines) and <=30 fps — 60 fps doubles the
    decode cost for a tiny tile. If nothing fits a tier, fall back
    gradually: playing big beats not playing.
    """
    if qmax and qmax > 0:
        return (
            "best[vcodec^=avc1][acodec!=none][height<=%d][fps<=?30]/"
            "best[vcodec!=none][acodec!=none][height<=%d][fps<=?30]/"
            "best[vcodec!=none][acodec!=none][height<=%d]/"
            "best[height<=%d]/"
            "best[vcodec!=none][acodec!=none]/best/"
            # Last resort: a video-only track. The wall starts muted, so a
            # silent tile is a far better outcome than an error tile.
            "bv*[height<=%d]/bv*"
        ) % (qmax, qmax, qmax, qmax, qmax)
    return "best[vcodec!=none][acodec!=none]/best/bv*"


class ResolveError(Exception):
    """yt-dlp couldn't resolve a stream. Carries a user-facing message + status."""

    def __init__(self, message, status=502):
        super().__init__(message)
        self.message = message
        self.status = status


class _YdlLogger:
    """Swallows yt-dlp's console output and keeps the last error.

    yt-dlp writes extractor failures straight to stderr even with quiet=True,
    so a wall of a dozen links buried the useful server log under red ERROR
    lines for failures we already report back to the browser.
    """

    def __init__(self):
        self.last_error = ""

    def debug(self, msg):
        pass

    def info(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        self.last_error = str(msg)


# Failure signatures worth explaining: the raw yt-dlp text alone leaves the
# user with nothing to act on. (substring, explanation) — first match wins.
_ERROR_HINTS = (
    ("phantomjs", "this site hides its stream behind JavaScript that yt-dlp "
                  "can only run with PhantomJS (unmaintained, not installed)"),
    ("drm", "the video is DRM-protected — it can only play on its own site"),
    ("sign in", "the site requires a logged-in session (cookies)"),
    ("login", "the site requires a logged-in session (cookies)"),
    ("private video", "the video is private"),
    ("geo-restricted", "the video is blocked in this region"),
    ("not available in your country", "the video is blocked in this region"),
    ("unsupported url", "yt-dlp has no extractor for this site"),
)


def _friendly_error(msg):
    """Turns a raw yt-dlp error into something the user can act on."""
    msg = (msg or "").strip() or "extraction failed"
    low = msg.lower()
    for needle, hint in _ERROR_HINTS:
        if needle in low:
            return "%s  →  %s" % (msg, hint)
    if "410" in low or "gone" in low or "unable to download webpage" in low:
        msg += "  →  yt-dlp may be outdated (pip install -U yt-dlp)"
        if _IMPERSONATE is None:
            msg += " or the site blocks non-browser clients (pip install -U curl_cffi)"
    elif ("403" in low or "forbidden" in low) and _IMPERSONATE is None:
        msg += "  →  site may block non-browser clients: pip install -U curl_cffi"
    return msg


# Failures a second, impersonated extraction cannot possibly fix. Keep this
# list tiny and only for causes that have nothing to do with HOW the page was
# fetched.
#
# "Unsupported URL" and "no video formats found" deliberately do NOT belong
# here, tempting as it looks: yt-dlp's generic extractor downloads the page and
# scrapes it, so a bot-protection page served to the plain client makes it find
# nothing and report exactly those errors. Impersonation is the thing that
# fixes them. Treating them as hopeless broke almost every site on this wall.
_IMPERSONATION_WONT_HELP = (
    "is not a valid url",   # malformed input; no fetch even happened
    "drm",                  # protected content stays protected
    "private video",        # a different TLS fingerprint is not a login
)


def _impersonation_may_help(msg):
    low = (msg or "").lower()
    return not any(sig in low for sig in _IMPERSONATION_WONT_HELP)


def _ydl_opts(qmax, logger):
    """Extraction options shared by every yt-dlp call.

    The timeouts matter as much as the format: without socket_timeout a single
    unresponsive CDN could hold an extraction slot for minutes while the rest
    of the wall waited behind it.
    """
    return {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "format": build_format(qmax),
        "logger": logger,
        "socket_timeout": 20,
        "retries": 2,
        "extractor_retries": 2,
    }


# Extraction failures that mean "you are asking too fast", not "this is
# broken". A wall of links from one site triggers these in bursts, and the only
# correct response is to wait and ask again.
_RATE_LIMIT_SIGNS = ("429", "too many requests", "rate limit", "rate-limit",
                     "slow down", "temporarily blocked", "try again later")
RESOLVE_RATE_RETRIES = 2


def _looks_rate_limited(msg):
    low = (msg or "").lower()
    return any(s in low for s in _RATE_LIMIT_SIGNS)


def _cookie_header(cookiejar, url):
    """The Cookie header a cookie jar would send to `url`, or None."""
    try:
        req = urllib.request.Request(url)
        cookiejar.add_cookie_header(req)
        return req.get_header("Cookie")
    except Exception:  # noqa: BLE001
        return None


# yt-dlp extractor classes, loaded once (the first sweep compiles ~1800
# _VALID_URL regexes, ~0.3s; every later check is a couple of ms).
_extractor_classes = None
_extractor_classes_lock = threading.Lock()


def _dedicated_extractor(url):
    """ie_key of the non-generic yt-dlp extractor claiming `url`, or None."""
    global _extractor_classes
    with _extractor_classes_lock:
        if _extractor_classes is None:
            from yt_dlp.extractor import gen_extractor_classes
            _extractor_classes = list(gen_extractor_classes())
    for ie in _extractor_classes:
        try:
            if ie.ie_key() != "Generic" and ie.suitable(url):
                return ie.ie_key()
        except Exception:  # noqa: BLE001 — one broken regex must not veto the rest
            pass
    return None


def _canonical_extractor_url(url):
    """`url` rewritten so a DEDICATED yt-dlp extractor recognises it, else `url`.

    Language/mobile subdomains (pt.eporner.com, de.site.com, m.site.com) serve
    the same video as www but usually sit outside the dedicated extractor's
    _VALID_URL (`(?:www\\.)?site\\.com`). yt-dlp then hands the page to its
    GENERIC extractor, which trusts whatever the page advertises — on many
    sites an SEO decoy the CDN refuses (see the decoy probe below). Swapping
    the subdomain for www. is only done when it makes a dedicated extractor
    match, so a host where the subdomain matters is never touched.
    """
    try:
        parts = urllib.parse.urlsplit(url)
        host = (parts.hostname or "").lower()
        labels = host.split(".")
        if len(labels) < 3 or labels[0] == "www":
            return url
        if _dedicated_extractor(url):
            return url
        for candidate_host in ("www." + ".".join(labels[1:]),
                               ".".join(labels[1:])):
            candidate = urllib.parse.urlunsplit(
                (parts.scheme, candidate_host, parts.path, parts.query,
                 parts.fragment))
            ie = _dedicated_extractor(candidate)
            if ie:
                sys.stderr.write("  RESOLVE CANON %s -> %s (extractor %s)\n"
                                 % (host, candidate_host, ie))
                return candidate
    except Exception:  # noqa: BLE001 — canonicalisation is best-effort
        pass
    return url


def resolve_stream(url, qmax, refresh=False):
    """Resolve a page/stream URL to a direct muxed (audio+video) stream.

    Returns a dict {title, stream_url, headers, isHls}. Results are cached (the
    same cache used by /api/resolve); `refresh` skips that cache, which is what
    a reader that just died mid-stream needs — the token it was handed has
    expired and re-reading it would fail the same way. Raises ImportError if
    yt-dlp is missing, or ResolveError on extraction failure.
    """
    cache_key = (url, qmax)
    now = time.time()
    with _resolve_lock:
        hit = _resolve_cache.get(cache_key)
        if hit and hit[0] > now and not refresh:
            return hit[1]

    import yt_dlp  # ImportError bubbles up to the caller

    # Cached under the URL the user gave; extracted from the URL a dedicated
    # extractor understands (pt.eporner.com -> www.eporner.com and friends).
    url = _canonical_extractor_url(url)

    logger = _YdlLogger()
    opts = _ydl_opts(qmax, logger)
    # Plain client first; impersonation only as a fallback (see
    # _setup_impersonation for why always-on impersonation breaks YouTube).
    info = None
    used_impersonation = False
    cookiejar = None
    e = None
    # Being told "too many requests" is not a failure, it's a request to wait.
    # Without this a burst of tiles from one site all gave up at once, which is
    # exactly how 17 good links turn into 2 playing tiles.
    for attempt in range(RESOLVE_RATE_RETRIES + 1):
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                cookiejar = ydl.cookiejar
            break
        except Exception as exc:  # noqa: BLE001
            e = exc
            if _IMPERSONATE is not None and _impersonation_may_help(str(exc)):
                try:
                    with yt_dlp.YoutubeDL({**opts, "impersonate": _IMPERSONATE}) as ydl:
                        info = ydl.extract_info(url, download=False)
                        cookiejar = ydl.cookiejar
                    used_impersonation = True
                    break
                except Exception as e2:  # noqa: BLE001
                    e = e2
            if attempt < RESOLVE_RATE_RETRIES and _looks_rate_limited(str(e)):
                time.sleep(1.5 * (attempt + 1) + 0.5 * attempt)
                continue
            break

    if info is None:
        raw = (str(e).splitlines()[-1] if e and str(e) else logger.last_error)
        reason = _friendly_error(raw)
        sys.stderr.write("  RESOLVE FAIL %s -> %s\n" % (url[:70], reason[:140]))
        raise ResolveError(reason)

    if "entries" in info:  # playlist -> take the first entry
        entries = [e for e in info["entries"] if e]
        if not entries:
            raise ResolveError("empty playlist")
        info = entries[0]

    stream_url = info.get("url")
    if not stream_url and info.get("requested_formats"):
        # A merge plan (separate video + audio). We can't mux while streaming,
        # so take the video track: the wall starts muted anyway, and a silent
        # tile beats a dead one.
        picked = next((f for f in info["requested_formats"]
                       if f.get("url") and f.get("vcodec") != "none"),
                      info["requested_formats"][0])
        stream_url = picked.get("url")
        info = {**info, **picked}
    if not stream_url and info.get("formats"):
        formats = [f for f in info["formats"] if f.get("url")]
        # Best muxed first, then anything with a video track.
        picked = next((f for f in reversed(formats)
                       if f.get("vcodec") != "none" and f.get("acodec") != "none"), None)
        picked = picked or next((f for f in reversed(formats)
                                 if f.get("vcodec") != "none"), None)
        if picked:
            stream_url = picked["url"]
            info = {**info, **picked}
    if not stream_url:
        raise ResolveError("no playable stream (may require audio/video merging or DRM)")

    # The generic extractor "smuggles" context into the URL it returns as a
    # #__youtubedl_smuggle fragment — meant for yt-dlp's own consumption, not
    # for a CDN. The Referer buried inside it is exactly what hotlink
    # protection checks, so lift the headers out before dropping the fragment.
    smuggled_headers = {}
    if "#__youtubedl_smuggle" in stream_url:
        try:
            from yt_dlp.utils import unsmuggle_url
            stream_url, smuggled = unsmuggle_url(stream_url, {})
            smuggled_headers = dict(smuggled.get("http_headers") or {})
            if smuggled.get("referer"):
                smuggled_headers.setdefault("Referer", smuggled["referer"])
        except Exception:  # noqa: BLE001
            stream_url = stream_url.split("#__youtubedl_smuggle", 1)[0]

    # yt-dlp expands an HLS master into per-variant formats by joining relative
    # URIs, and RFC 3986 joining DROPS the parent's query — so on a signed CDN
    # the URL it hands back has lost the very signature that makes it
    # fetchable, and every request 403s. It keeps the signed original in
    # `manifest_url`; put that signature back on the variant we chose.
    manifest = info.get("manifest_url")
    if manifest and not urllib.parse.urlsplit(stream_url).query:
        if _looks_signed(urllib.parse.urlsplit(manifest).query):
            stream_url = retoken(stream_url, manifest) or stream_url

    headers = dict(info.get("http_headers") or {})
    for k, v in smuggled_headers.items():
        headers.setdefault(k, v)
    headers.setdefault("User-Agent", DEFAULT_UA)
    # yt-dlp collects cookies while walking the site, and plenty of CDNs check
    # them before handing over the media (the stream URL alone is not enough).
    # Those cookies live in yt-dlp's jar and never reached our proxy, so every
    # fetch looked like a stranger and came back 403.
    if cookiejar is not None and "Cookie" not in headers:
        cookie = _cookie_header(cookiejar, stream_url)
        if cookie:
            headers["Cookie"] = cookie
    result = {
        "title": info.get("title") or info.get("webpage_url_basename") or url,
        "stream_url": stream_url,
        "headers": headers,
        # Carried through because indexing needs it to turn "the last third"
        # into seconds; a listing page does not report it.
        "duration": float(info.get("duration") or 0),
        "isHls": is_hls(stream_url, info.get("protocol", "")),
        "impersonated": used_impersonation,
    }

    # The generic extractor trusts whatever the page ADVERTISES (JSON-LD
    # contentUrl, og:video) — on plenty of sites that is an SEO decoy the CDN
    # refuses to serve to anyone (eporner's gvideo.* answers 403 even to a
    # real browser). Real extractors return URLs the site actually plays, so
    # only generic results get this probe. Catching the decoy here turns a
    # dead cached token (which no proxy healing can revive — re-extraction
    # returns the same decoy) into a resolve error the front-end can act on,
    # e.g. by falling back to the stream the Tabs extension sniffed.
    if info.get("extractor_key") == "Generic" and not result["isHls"]:
        st, ctype, _body, perr = _probe(
            stream_url, {**headers, "Range": "bytes=0-2047"}, used_impersonation)
        media_path = urllib.parse.urlsplit(stream_url).path.lower()
        looks_media = media_path.endswith((".mp4", ".webm", ".m4v", ".mov", ".ogv"))
        decoy = (st in (401, 403, 404, 410) or
                 (st == 200 and looks_media and "text/html" in (ctype or "").lower()))
        if decoy:
            host = urllib.parse.urlsplit(stream_url).netloc
            sys.stderr.write("  RESOLVE DECOY %s -> HTTP %s from %s\n"
                             % (url[:70], st, host))
            raise ResolveError(
                "the only stream this page advertises is refused by its CDN "
                "(HTTP %s from %s) — likely an SEO decoy (JSON-LD/og:video) "
                "and yt-dlp has no dedicated extractor for this site" % (st, host))

    with _resolve_lock:
        _resolve_cache[cache_key] = (now + RESOLVE_TTL, result)
        if len(_resolve_cache) > 200:
            for k in [k for k, v in _resolve_cache.items() if v[0] <= now]:
                del _resolve_cache[k]
    return result


def _fetch_page_html(page_url):
    """Downloads a page's HTML for scanning. Falls back to browser
    impersonation when the plain client is blocked. Returns (html, used_imp)."""
    headers = {
        "User-Agent": DEFAULT_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
    }
    max_bytes = 4 * 1024 * 1024  # HTML pages beyond this are not worth scanning
    # Short timeouts on purpose: the scan is a bonus running next to the real
    # extraction, so a slow page must never become the tile's critical path.
    try:
        if _HAS_REQUESTS:
            sess = _acquire_session()
            try:
                r = sess.get(page_url, headers=headers, timeout=(5, 8),
                             allow_redirects=True)
            finally:
                _release_session(sess)
            if r.status_code >= 400:
                raise urllib.error.HTTPError(page_url, r.status_code,
                                             "upstream error", None, None)
            return r.content[:max_bytes].decode("utf-8", "replace"), False
        req = urllib.request.Request(page_url, headers=headers)
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.read(max_bytes).decode("utf-8", "replace"), False
    except Exception as e:  # noqa: BLE001
        if _IMPERSONATE is None:
            raise ResolveError("couldn't fetch the page: %s" % e)
        try:
            from curl_cffi import requests as curl_requests
            r = curl_requests.get(page_url, headers=headers, timeout=8,
                                  impersonate="chrome")
            if r.status_code >= 400:
                raise ResolveError("page returned HTTP %d" % r.status_code)
            return r.content[:max_bytes].decode("utf-8", "replace"), True
        except ResolveError:
            raise
        except Exception as e2:  # noqa: BLE001
            raise ResolveError("couldn't fetch the page: %s" % e2)


_SRC_ATTR_RE = re.compile(r'\bsrc\s*=\s*["\']([^"\']+)["\']', re.I)
_TYPE_ATTR_RE = re.compile(r'\btype\s*=\s*["\']([^"\']+)["\']', re.I)
_SOURCE_TAG_RE = re.compile(r"<source\b[^>]*>", re.I)
_VIDEO_OPEN_RE = re.compile(r"<video\b[^>]*>", re.I)
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)

# Streams that never show up as a <video src>. An MSE player (hls.js, dash.js,
# shaka…) creates a MediaSource, hands the element a blob: handle and pushes
# segments into it from JS — so the element's src is a pointer into that tab's
# memory and the only trace of the real stream in the HTML is the manifest URL
# sitting in the player's config. Inside JSON/JS it is usually escaped as
# https:\/\/…, hence the optional backslashes.
_MANIFEST_RE = re.compile(
    r"""https?:\\?/\\?/[^\s"'<>()]+?\.(?:m3u8|mpd)(?:\?[^\s"'<>()]*)?""", re.I)


_IFRAME_RE = re.compile(r"<iframe\b[^>]*>", re.I)

# Iframes that are never a video player: ads, analytics, comments, and the big
# embeds the front-end handles natively as iframes of their own.
_SKIP_FRAME_HOSTS = (
    "googletagmanager", "google-analytics", "doubleclick", "googlesyndication",
    "googleadservices", "adservice.", "facebook.", "connect.facebook",
    "twitter.", "x.com", "disqus.", "recaptcha", "gstatic.", "youtube.",
    "youtu.be", "vimeo.", "dailymotion.", "twitch.tv",
)
MAX_FRAMES = 3  # player iframes to open before giving up on a page


def _videos_in_html(text, base_url):
    """Media URLs one document offers, in the order a player would prefer.

    A <video> can carry its stream in a src attribute or in child <source>
    tags; multiple <source> children are quality/format ALTERNATES of the same
    video, so only the first playable one counts.
    """
    import html as _html

    videos, seen = [], set()

    def add(candidate):
        """Records a playable candidate; returns True when it was usable."""
        candidate = (candidate or "").strip()
        if not candidate or candidate.startswith(("blob:", "data:")):
            return False
        absolute = urllib.parse.urljoin(base_url, _html.unescape(candidate))
        if absolute not in seen:
            seen.add(absolute)
            videos.append(absolute)
        return True

    for m in _VIDEO_OPEN_RE.finditer(text):
        open_tag = m.group(0)
        end = text.find("</video", m.end())
        block = text[m.end(): end if end != -1 else m.end() + 5000]

        sm = _SRC_ATTR_RE.search(open_tag)
        if sm and add(sm.group(1)):
            continue
        for st in _SOURCE_TAG_RE.finditer(block):
            tag = st.group(0)
            ty = _TYPE_ATTR_RE.search(tag)
            if ty and not ty.group(1).lower().strip().startswith("video/"):
                continue
            sm = _SRC_ATTR_RE.search(tag)
            if sm and add(sm.group(1)):
                break

    # Not a single usable <video src> normally means an MSE player: at runtime
    # the element gets a blob: URL, which is worthless to us. The manifest its
    # JS is about to load is usually right there in the page as plain text.
    # Only the first one counts — the others are its quality variants, not
    # separate videos.
    if not videos:
        for m in _MANIFEST_RE.finditer(text):
            if add(_html.unescape(m.group(0)).replace("\\/", "/")):
                break

    return videos


def _player_frames(page_url, text, prefer_origin=""):
    """Iframes worth opening in search of a player, most promising first.

    Embedding the player from another host is the norm, not the exception: the
    page in the address bar is a shell and the <video> lives in an iframe on a
    site nobody links to directly. `prefer_origin` is where a blob: URL came
    from, which names that host exactly — when the user hands it to us, the
    right frame goes first.
    """
    import html as _html

    frames, seen = [], set()
    for m in _IFRAME_RE.finditer(text):
        sm = _SRC_ATTR_RE.search(m.group(0))
        if not sm:
            continue
        url = urllib.parse.urljoin(page_url, _html.unescape(sm.group(1).strip()))
        if not url.lower().startswith(("http://", "https://")):
            continue
        host = (urllib.parse.urlparse(url).hostname or "").lower()
        if any(skip in host for skip in _SKIP_FRAME_HOSTS):
            continue
        if url not in seen:
            seen.add(url)
            frames.append(url)

    want = prefer_origin.rstrip("/").lower()
    if want:
        frames.sort(key=lambda u: 0 if u.lower().startswith(want) else 1)
    return frames[:MAX_FRAMES]


def scan_page_videos(page_url, prefer_origin=""):
    """Finds the videos a page offers: one media URL per <video>, the player's
    manifest when the <video> is fed by JS, and — when the page itself holds
    nothing — the same search inside its player iframes.

    Returns (page_title, [absolute media urls], used_impersonation, referer),
    where `referer` is the document the URLs were found in: hotlink protection
    checks it, and for an embedded player that is the frame, not the page.

    Cached like resolves: a tile that retries or changes quality asks again,
    and re-downloading the same page each time is pure latency.
    """
    now = time.time()
    key = (page_url, prefer_origin)
    with _scan_lock:
        hit = _scan_cache.get(key)
        if hit and hit[0] > now:
            return hit[1]

    text, used_imp = _fetch_page_html(page_url)

    tm = _TITLE_RE.search(text)
    page_title = re.sub(r"\s+", " ", tm.group(1)).strip() if tm else ""

    videos = _videos_in_html(text, page_url)
    referer = page_url

    if not videos:
        for frame_url in _player_frames(page_url, text, prefer_origin):
            try:
                frame_text, frame_imp = _fetch_page_html(frame_url)
            except Exception:  # noqa: BLE001 — a dead frame just isn't the one
                continue
            found = _videos_in_html(frame_text, frame_url)
            if found:
                videos, referer, used_imp = found, frame_url, frame_imp
                if not page_title:
                    ftm = _TITLE_RE.search(frame_text)
                    page_title = (re.sub(r"\s+", " ", ftm.group(1)).strip()
                                  if ftm else "")
                break

    result = (page_title, videos, used_imp, referer)
    with _scan_lock:
        _scan_cache[key] = (now + SCAN_TTL, result)
        if len(_scan_cache) > 200:
            for k in [k for k, v in _scan_cache.items() if v[0] <= now]:
                del _scan_cache[k]
    return result


def _is_direct_media(url):
    """True for plain media URLs/paths ffmpeg can open by itself (mp4/m3u8/…)."""
    return bool(_DIRECT_MEDIA_RE.search(url.split("#")[0]))


def _ffmpeg_input_opts(stream, headers):
    """Input options carrying HTTP headers ffmpeg needs (Referer/UA/Cookie).

    These are options of the http(s) protocol, so they only apply to network
    inputs — passing them to a local file makes ffmpeg abort with
    "Option not found". Return nothing for non-http streams.
    """
    if not str(stream).lower().startswith(("http://", "https://")):
        return []
    opts = ["-user_agent", (headers or {}).get("User-Agent") or DEFAULT_UA]
    extra = "".join(
        "%s: %s\r\n" % (k, v)
        for k, v in (headers or {}).items()
        if v and k.lower() != "user-agent")
    if extra:
        opts += ["-headers", extra]
    return opts


# Serialize downloads per source URL: fetching the same video from several
# threads at once trips rate-limiting/403s (and the user's main use case is many
# cuts from ONE video). Different videos still download in parallel.
_dl_locks = {}
_dl_locks_guard = threading.Lock()


def _reconnect_opts(src):
    """Tell ffmpeg to reconnect instead of stopping when an http read drops.

    A CDN closing the socket halfway through a long read used to end a
    measuring pass silently: ffmpeg exits, the frames it did produce look like
    a complete video, and the cached result covers the first thirty seconds of
    a fourteen minute film. These options make it retry the read.
    """
    if not str(src).lower().startswith(("http://", "https://")):
        return []
    return ["-reconnect", "1", "-reconnect_streamed", "1",
            "-reconnect_on_network_error", "1", "-reconnect_delay_max", "10",
            "-rw_timeout", "30000000"]


def _lock_for_url(url):
    with _dl_locks_guard:
        lk = _dl_locks.get(url)
        if lk is None:
            lk = threading.Lock()
            _dl_locks[url] = lk
        return lk


def _download_section(url, start, end, qmax, tmpdir, index):
    """Download only the [start,end] span of `url` with yt-dlp.

    yt-dlp manages the whole site session (tokens, throttling, DASH, merging
    separate audio+video), so this reliably works for YouTube & friends where
    handing a raw stream URL to ffmpeg gets a 403. `download_ranges` fetches just
    the needed span (cut at the nearest keyframe — a few tenths of a second of
    tolerance, which the later re-encode absorbs). We deliberately avoid
    `force_keyframes_at_cuts`: its extra in-download re-encode crashes some
    ffmpeg builds, and we re-encode during normalization anyway. Returns the
    produced file path, or None.
    """
    import yt_dlp
    from yt_dlp.utils import download_range_func

    url = _canonical_extractor_url(url)
    outtmpl = os.path.join(tmpdir, "src_%03d.%%(ext)s" % index)
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "noplaylist": True,
        "logger": _YdlLogger(),
        "format": build_format(qmax),
        "outtmpl": {"default": outtmpl},
        "download_ranges": download_range_func(None, [(start, end)]),
        "retries": 3,
        "fragment_retries": 3,
    }
    if _IMPERSONATE is not None:
        opts["impersonate"] = _IMPERSONATE
    if FFMPEG_DIR:
        opts["ffmpeg_location"] = FFMPEG_DIR

    prefix = "src_%03d." % index

    def produced():
        for name in os.listdir(tmpdir):
            if name.startswith(prefix):
                return os.path.join(tmpdir, name)
        return None

    # The ffmpeg-based section downloader crashes intermittently on some builds
    # (and CDNs occasionally 403 a hot request). Both are transient, so retry a
    # few times — this takes the real-world failure rate to near zero.
    last_err = None
    with _lock_for_url(url):
        for attempt in range(3):
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    ydl.download([url])
                out = produced()
                if out and os.path.getsize(out) > 0:
                    return out
            except Exception as e:  # noqa: BLE001
                last_err = e
            # Clear any partial artefacts before the next attempt.
            for name in list(os.listdir(tmpdir)):
                if name.startswith(prefix):
                    try:
                        os.remove(os.path.join(tmpdir, name))
                    except OSError:
                        pass
    if last_err:
        raise last_err
    return None


# Whether a source carries sound, remembered per source: the concat step joins
# clips with `-c copy`, and a clip with no audio track next to clips that have
# one loses the sound of the whole compilation.
_audio_probe = {}
_audio_probe_lock = threading.Lock()


def _source_has_audio(source, headers):
    """True/False, or None when the probe itself failed (then don't force maps)."""
    key = str(source)
    with _audio_probe_lock:
        if key in _audio_probe:
            return _audio_probe[key]
    cmd = [FFMPEG, "-hide_banner"] + _ffmpeg_input_opts(source, headers) + ["-i", source]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
        err = r.stderr or ""
        found = bool(_AUD_RE.search(err)) if _VID_RE.search(err) else None
    except Exception:  # noqa: BLE001
        found = None
    with _audio_probe_lock:
        _audio_probe[key] = found
        if len(_audio_probe) > 400:
            _audio_probe.clear()
    return found


def _normalize_clip(source, headers, seek, dur, out, resolution, encoder):
    """Re-encode `source` to a uniform MPEG-TS clip so all clips concat cleanly.

    seek/dur trim the input (used for direct fast-seek); pass None/None when the
    input is already an exact clip. Tries the chosen (hardware) encoder and falls
    back to libx264 if it fails. Returns (ok, error_message).
    """
    w, h = resolution
    vf = ("scale=%d:%d:force_original_aspect_ratio=decrease,"
          "pad=%d:%d:(ow-iw)/2:(oh-ih)/2:color=black,fps=30,format=yuv420p"
          % (w, h, w, h))

    # A silent source gets a silent track, so every clip has exactly one video
    # and one audio stream and the concat can stay a copy.
    silent = _source_has_audio(source, headers) is False

    def build(enc):
        cmd = [FFMPEG, "-y", "-hide_banner", "-loglevel", "error"]
        if seek is not None:
            cmd += ["-ss", "%.3f" % seek]
        cmd += _ffmpeg_input_opts(source, headers)
        cmd += _reconnect_opts(source)
        cmd += ["-i", source]
        if silent:
            cmd += ["-f", "lavfi", "-i",
                    "anullsrc=channel_layout=stereo:sample_rate=48000",
                    "-map", "0:v:0", "-map", "1:a:0", "-shortest"]
        if dur is not None:
            cmd += ["-t", "%.3f" % dur]
        cmd += ["-vf", vf]
        cmd += _venc_args(enc)
        cmd += ["-c:a", "aac", "-ar", "48000", "-ac", "2", "-b:a", "160k"]
        cmd += ["-fps_mode", "cfr", "-f", "mpegts", out]
        return cmd

    encoders = [encoder] if encoder == "libx264" else [encoder, "libx264"]
    last_err = ""
    for enc in encoders:
        try:
            r = subprocess.run(build(enc), capture_output=True, timeout=900)
            if r.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 0:
                return True, ""
            tail = (r.stderr or b"").decode("utf-8", "replace").strip().splitlines()
            last_err = tail[-1] if tail else "ffmpeg exit %d" % r.returncode
        except Exception as e:  # noqa: BLE001
            last_err = str(e)
    return False, last_err


def extract_cut(index, cut, tmpdir, resolution, qmax, encoder):
    """Produce one normalized MPEG-TS clip for a {url,start,end} cut.

    Three ways in, in order of how well they hold up:

    1. Direct media or a local file → ffmpeg seeks straight to the span.
    2. A page → resolve it (the cache is usually warm) and let ffmpeg seek into
       the stream. This is the same route indexing takes, and on sites that
       answer a second extraction with a challenge page it is the one that
       keeps working: no new extraction, no download of the whole file.
    3. Only then yt-dlp's own section download, which handles DASH and merging
       but asks the site for everything again.

    Returns (index, path_or_None, error_message).
    """
    url = cut["url"]
    start = cut["start"]
    dur = cut["end"] - cut["start"]
    out = os.path.join(tmpdir, "seg_%03d.ts" % index)
    problems = []

    if _is_direct_media(url) or not url.lower().startswith(("http://", "https://")):
        ok, err = _normalize_clip(url, {}, start, dur, out, resolution, encoder)
        if ok:
            return index, out, ""
        problems.append(err)
        # A local path that failed cannot be recovered any other way.
        if not url.lower().startswith(("http://", "https://")):
            return index, None, err

    if url.lower().startswith(("http://", "https://")):
        try:
            info = resolve_stream(url, qmax)
            ok, err = _normalize_clip(info["stream_url"], info.get("headers") or {},
                                      start, dur, out, resolution, encoder)
            if ok:
                return index, out, ""
            problems.append(err)
        except Exception as e:  # noqa: BLE001
            problems.append((str(e).splitlines() or ["resolve failed"])[-1])

    try:
        src = _download_section(url, start, cut["end"], qmax, tmpdir, index)
    except Exception as e:  # noqa: BLE001
        problems.append((str(e).splitlines() or ["download failed"])[-1])
        return index, None, " | ".join(p[:90] for p in problems[-2:])
    if not src:
        problems.append("yt-dlp produced no clip file")
        return index, None, " | ".join(p[:90] for p in problems[-2:])

    ok, err = _normalize_clip(src, {}, None, None, out, resolution, encoder)
    try:
        os.remove(src)
    except OSError:
        pass
    if ok:
        return index, out, ""
    problems.append(err)
    return index, None, " | ".join(p[:90] for p in problems[-2:])


def _set_job(job_id, **fields):
    with _compile_jobs_lock:
        job = _compile_jobs.get(job_id)
        if job:
            job.update(fields)


def _run_compile_job(job_id, cuts, resolution, qmax):
    """Background worker: extract every clip, concatenate, update job progress."""
    tmpdir = tempfile.mkdtemp(prefix="mscompile_")
    _set_job(job_id, tmpdir=tmpdir, stage="extracting")
    try:
        segs = [None] * len(cuts)
        errors = []
        with ThreadPoolExecutor(max_workers=COMPILE_WORKERS) as ex:
            futures = [ex.submit(extract_cut, i, c, tmpdir, resolution, qmax, VIDEO_ENCODER)
                       for i, c in enumerate(cuts)]
            for fut in as_completed(futures):
                idx, path, err = fut.result()
                segs[idx] = path
                if not path and err:
                    errors.append("cut %d: %s" % (idx + 1, err))
                with _compile_jobs_lock:
                    job = _compile_jobs.get(job_id)
                    if job:
                        job["done"] += 1

        good = [s for s in segs if s]
        if not good:
            _set_job(job_id, stage="error",
                     error="could not extract any clip. " + " | ".join(errors[:3]))
            return

        _set_job(job_id, stage="concatenating")
        listfile = os.path.join(tmpdir, "list.txt")
        with open(listfile, "w", encoding="utf-8") as f:
            for s in segs:
                if s:
                    safe = s.replace("\\", "/").replace("'", "'\\''")
                    f.write("file '%s'\n" % safe)
        out = os.path.join(tmpdir, "compilation.mp4")
        cc = [FFMPEG, "-y", "-hide_banner", "-loglevel", "error",
              "-f", "concat", "-safe", "0", "-i", listfile,
              "-c", "copy", "-movflags", "+faststart", out]
        r = subprocess.run(cc, capture_output=True, timeout=900)
        if r.returncode != 0 or not os.path.exists(out) or os.path.getsize(out) == 0:
            detail = (r.stderr or b"").decode("utf-8", "replace")[-300:]
            _set_job(job_id, stage="error", error="concat failed: " + detail)
            return

        _set_job(job_id, stage="done", result=out, skipped=len(cuts) - len(good))
    except Exception as e:  # noqa: BLE001
        _set_job(job_id, stage="error", error=str(e)[:200])


# ---------- /api/shrink (re-encode a video to the size it is shown at) ----------

# A 4K60 tile costs the GPU ~500 Mpixel/s to decode and then lands on a cell of
# ~400px: almost every decoded pixel is thrown away by the scaler. A 3060 has a
# single NVDEC engine that saturates at one or two 4K60 H.264 streams, so a wall
# with ten of them stalls no matter how the bytes were obtained — which is why
# packing the videos locally (no network, no yt-dlp) did not help.
#
# Shrinking re-encodes each video once, offline, to the resolution the tile
# actually shows, at <=30 fps and 8-bit yuv420p. Twenty tiles at 768x432/30 add
# up to less decode work than a single 4K60 stream.

# The tier the browser asks for comes from the wall's own geometry; this is
# only the default when a caller leaves it out.
SHRINK_FPS = 30
# NVENC sessions. More does not go faster (one encode engine) and consumer
# drivers cap concurrent sessions anyway; two keeps the engine fed while one
# process is still demuxing.
SHRINK_WORKERS = 2
SHRINK_JOB_TTL = 1800

_shrink_jobs = {}
_shrink_jobs_lock = threading.Lock()
_shrink_slots = threading.BoundedSemaphore(SHRINK_WORKERS)


def _set_shrink(job_id, **fields):
    with _shrink_jobs_lock:
        job = _shrink_jobs.get(job_id)
        if job:
            job.update(fields)


_DUR_RE = re.compile(r"Duration:\s*(\d+):(\d+):([\d.]+)")
_VID_RE = re.compile(
    r"Stream #\d+:\d+.*?: Video: (\w+)[^\n]*?\b(\d{2,5})x(\d{2,5})\b[^\n]*")
_FPS_RE = re.compile(r"([\d.]+) fps")
_AUD_RE = re.compile(r"Stream #\d+:\d+.*?: Audio: ")


def probe_media(path):
    """Video geometry of a local file, parsed from `ffmpeg -i` (the
    imageio-ffmpeg bundle ships no ffprobe). None if it isn't readable."""
    if not FFMPEG:
        return None
    try:
        r = subprocess.run([FFMPEG, "-hide_banner", "-i", path],
                           capture_output=True, text=True, timeout=60)
    except Exception:  # noqa: BLE001
        return None
    err = r.stderr or ""
    vm = _VID_RE.search(err)
    if not vm:
        return None
    dm = _DUR_RE.search(err)
    fm = _FPS_RE.search(err[vm.start():vm.end() + 200])
    return {
        "codec": vm.group(1),
        "w": int(vm.group(2)),
        "h": int(vm.group(3)),
        "fps": float(fm.group(1)) if fm else 0.0,
        "dur": (int(dm.group(1)) * 3600 + int(dm.group(2)) * 60
                + float(dm.group(3))) if dm else 0.0,
        "audio": bool(_AUD_RE.search(err)),
    }


def _shrink_target(info, hbox):
    """(w, h) the video ends up at inside an `hbox`-tall box, aspect kept and
    never upscaled. The width bound only bites on wider-than-21:9 sources
    (side-by-side renders), which carry far more pixels than their height
    suggests."""
    wbox = (int(hbox * 21 / 9) // 2) * 2
    w, h = info["w"], info["h"]
    scale = min(1.0, wbox / float(w), hbox / float(h))
    return max(2, (int(w * scale) // 2) * 2), max(2, (int(h * scale) // 2) * 2)


def _shrink_filters(info, hbox, fps_cap):
    """Filter chain + target size. fps is capped only when the source is above
    it (upsampling 24 fps to 30 duplicates frames and grows the file for
    nothing), scale only when it is bigger, and the output always lands on
    8-bit yuv420p — a 10-bit/HDR source has no hardware decoder in the browser
    and silently falls back to software."""
    tw, th = _shrink_target(info, hbox)
    vf = []
    if info["fps"] > fps_cap + 0.5:
        vf.append("fps=%d" % fps_cap)
    if (tw, th) != (info["w"], info["h"]):
        vf.append("scale=%d:%d:flags=bicubic" % (tw, th))
    vf.append("format=yuv420p")
    return ",".join(vf), tw, th


def _shrink_needed(info, hbox, fps_cap):
    _, tw, th = _shrink_filters(info, hbox, fps_cap)
    return (tw, th) != (info["w"], info["h"]) or info["fps"] > fps_cap + 0.5


def _forget_upload(path):
    """Drop an uploaded file once its shrunk replacement exists. Only used for
    uploads made for shrinking, never for a file a tile is still playing."""
    with _uploads_lock:
        for fid, p in list(_uploads.items()):
            if p == path:
                _uploads.pop(fid, None)
    try:
        os.remove(path)
    except OSError:
        pass


def _run_shrink_job(job_id, src, out, info, hbox, fps_cap, drop_src):
    """Background worker: re-encode `src` into `out`, publishing % as it goes.

    Tries the hardware encoder with CUDA decoding first, then a plain software
    pass, so a driver refusing another NVENC session (or a codec NVDEC can't
    take) still produces a file."""
    vf, tw, th = _shrink_filters(info, hbox, fps_cap)
    dur = info["dur"] or 0.0
    _set_shrink(job_id, to={"w": tw, "h": th,
                            "fps": min(float(fps_cap), info["fps"] or fps_cap)})

    def attempt(encoder, hwaccel):
        cmd = [FFMPEG, "-y", "-hide_banner", "-nostdin", "-loglevel", "error"]
        if hwaccel:
            cmd += ["-hwaccel", "cuda"]
        cmd += ["-i", src, "-map", "0:v:0", "-vf", vf]
        cmd += _venc_args(encoder)
        if info["audio"]:
            cmd += ["-map", "0:a:0?", "-c:a", "aac", "-b:a", "128k",
                    "-ac", "2", "-ar", "48000"]
        else:
            cmd += ["-an"]
        cmd += ["-movflags", "+faststart", "-progress", "pipe:1", out]

        # -progress writes to stdout; ffmpeg's own errors go to a file, so the
        # pipe has a single reader and can't deadlock.
        errpath = out + ".log"
        with open(errpath, "wb") as errf:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=errf)
            try:
                for raw in proc.stdout:
                    line = raw.decode("utf-8", "replace").strip()
                    if line.startswith("out_time_us=") and dur > 0:
                        try:
                            secs = int(line.split("=", 1)[1]) / 1e6
                        except ValueError:
                            continue
                        _set_shrink(job_id, pct=max(0.0, min(0.99, secs / dur)))
                    elif line.startswith("total_size="):
                        try:
                            _set_shrink(job_id, bytes=int(line.split("=", 1)[1]))
                        except ValueError:
                            pass
            finally:
                proc.stdout.close()
                proc.wait(timeout=120)
        try:
            with open(errpath, "rb") as f:
                tail = f.read().decode("utf-8", "replace").strip().splitlines()
            os.remove(errpath)
        except OSError:
            tail = []
        if proc.returncode == 0 and os.path.isfile(out) and os.path.getsize(out) > 0:
            return True, ""
        return False, (tail[-1] if tail else "ffmpeg exit %d" % proc.returncode)

    attempts = [(VIDEO_ENCODER, VIDEO_ENCODER == "h264_nvenc")]
    if VIDEO_ENCODER != "libx264":
        attempts.append(("libx264", False))
    err = "shrink not attempted"
    for encoder, hwaccel in attempts:
        _set_shrink(job_id, stage="encoding", encoder=encoder, pct=0.0)
        try:
            ok, err = attempt(encoder, hwaccel)
        except Exception as e:  # noqa: BLE001
            ok, err = False, str(e)[:200]
        if not ok:
            continue
        fid = uuid.uuid4().hex
        final = os.path.join(_UPLOAD_DIR, fid + ".mp4")
        try:
            os.replace(out, final)
        except OSError as e:
            _set_shrink(job_id, stage="error", error="could not store: %s" % e)
            return
        with _uploads_lock:
            _uploads[fid] = final
        if drop_src:
            _forget_upload(src)
        _set_shrink(job_id, stage="done", pct=1.0,
                    url="/api/localfile/%s/file.mp4" % fid,
                    bytes=os.path.getsize(final))
        return

    for leftover in (out, out + ".log"):
        if os.path.exists(leftover):
            try:
                os.remove(leftover)
            except OSError:
                pass
    _set_shrink(job_id, stage="error", error=(err or "encode failed")[:300])


# ---------- /api/analyze (find the interesting moments, ffmpeg only) ----------

# Marking a compilation by hand means watching everything first. This measures
# the video instead, with no model and nothing paid: ONE ffmpeg pass, decoded at
# 4 fps and 160 px wide, printing two cheap per-frame numbers — scdet's `mafd`
# (how much the picture changed since the previous frame, i.e. motion) and its
# scene score (a hard cut) — next to EBU R128 momentary loudness from the audio.
# Peaks in loudness and motion are where something happens; scene cuts are where
# a clip can start and end without slicing a shot in half.
#
# The server only produces the curves. The browser picks the peaks, so retuning
# clip length or the audio/motion balance is instant instead of another decode.

ANALYZE_FPS = 4
ANALYZE_WIDTH = 160
# Decoding competes with the wall's own playback; two at a time keeps the
# machine usable while a batch of tiles is measured.
ANALYZE_WORKERS = 2
ANALYZE_JOB_TTL = 1800
# Everything measured about a video — curves here, CLIP vectors later — is
# expensive to produce and tiny to keep, so it lives in a real cache directory
# instead of TEMP, where a cleanup would throw it away.
# Where the measured curves, the CLIP models and the indexes live. Normally in
# the user's own profile, but a second account on the same machine should point
# at the first one's copy instead of fetching another 2.8 GB of identical
# models: set MULTISCREEN_CACHE and both share one.
MS_CACHE_HOME = (os.environ.get("MULTISCREEN_CACHE")
                 or os.path.join(os.path.expanduser("~"), ".cache", "multiscreen"))
ANALYZE_CACHE_DIR = os.path.join(MS_CACHE_HOME, "curves")
ANALYZE_CACHE_TTL = 14 * 86400
# Below this a scene score is camera shake or a flash, not a cut.
ANALYZE_SCENE_MIN = 8.0
# R128 reports digital silence as -120 LUFS; clamping keeps one silent gap from
# flattening the normalisation of everything else.
ANALYZE_SILENCE = -70.0
# A full pass over a very long video is minutes of decoding — measure the first
# four hours and stop.
ANALYZE_MAX_SECONDS = 4 * 3600

_analyze_jobs = {}
_analyze_jobs_lock = threading.Lock()
_analyze_slots = threading.BoundedSemaphore(ANALYZE_WORKERS)


def _set_analyze(job_id, **fields):
    with _analyze_jobs_lock:
        job = _analyze_jobs.get(job_id)
        if job:
            job.update(fields)


def _hms(seconds):
    seconds = int(seconds or 0)
    return "%d:%02d" % (seconds // 60, seconds % 60)


def _analyze_cache_path(url, qmax):
    key = hashlib.sha1(("%s|%s" % (url, qmax)).encode("utf-8")).hexdigest()
    return os.path.join(ANALYZE_CACHE_DIR, key + ".json")


def _analyze_cache_get(url, qmax):
    path = _analyze_cache_path(url, qmax)
    try:
        if time.time() - os.path.getmtime(path) > ANALYZE_CACHE_TTL:
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _analyze_cache_put(url, qmax, data):
    try:
        os.makedirs(ANALYZE_CACHE_DIR, exist_ok=True)
        with open(_analyze_cache_path(url, qmax), "w", encoding="utf-8") as f:
            json.dump(data, f)
    except (OSError, ValueError, TypeError):
        pass


def _analysis_source(url, qmax, refresh=False):
    """What ffmpeg should open to measure `url`, as (source, headers, title).

    A tile playing a local upload (or a restored package) is read straight off
    disk — no network, no yt-dlp, and the pass runs at disk speed. A plain media
    URL goes to ffmpeg as it is; anything else (a video page) goes through the
    same resolve the player itself uses, which also carries the Referer/Cookie
    headers the CDN wants.
    """
    m = re.search(r"/api/localfile/([0-9a-zA-Z]+)", url)
    if m:
        fid = m.group(1)
        with _uploads_lock:
            path = _uploads.get(fid)
        if not path or not os.path.isfile(path):
            path = _recover_upload(fid)
        if path:
            return path, {}, os.path.basename(path)

    if not url.lower().startswith(("http://", "https://")):
        if os.path.isfile(url):
            return url, {}, os.path.basename(url)

    if _is_direct_media(url):
        return url, {}, url

    info = resolve_stream(url, qmax, refresh=refresh)
    return info["stream_url"], info.get("headers") or {}, info.get("title") or url


def _parse_metadata_log(path, keys):
    """Read an ffmpeg `metadata=print` log into {key: [(pts_time, value), …]}.

    The filter prints a `frame:` header followed by one `key=value` line per
    metadata entry on that frame, so the parse is a tiny state machine rather
    than one regex — other filters in the chain may add keys of their own.
    """
    out = {k: [] for k in keys}
    when = None
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if line.startswith("frame:"):
                    m = re.search(r"pts_time:([\d.]+)", line)
                    when = float(m.group(1)) if m else None
                elif when is not None and "=" in line:
                    key, _, val = line.strip().partition("=")
                    if key in out:
                        try:
                            out[key].append((when, float(val)))
                        except ValueError:
                            pass
    except OSError:
        pass
    return out


def _analyze_pass(src, headers, tmpdir, want_audio, on_progress):
    """Run the measuring pass over `src`. Returns (returncode, ffmpeg log).

    Both metadata sinks write bare filenames into `tmpdir`, which is also the
    process's working directory: a filter argument cannot carry a Windows path
    without escaping the drive-letter colon twice over, and this sidesteps it.
    """
    graph = ("[0:v]fps=%d,scale=%d:-2,scdet=threshold=%.1f,"
             "metadata=print:file=video.txt[v]"
             % (ANALYZE_FPS, ANALYZE_WIDTH, ANALYZE_SCENE_MIN))
    maps = ["-map", "[v]"]
    if want_audio:
        graph += (";[0:a]ebur128=metadata=1:peak=none,"
                  "ametadata=print:key=lavfi.r128.M:file=audio.txt[a]")
        maps += ["-map", "[a]"]

    cmd = [FFMPEG, "-y", "-hide_banner", "-nostats", "-progress", "pipe:1"]
    cmd += _ffmpeg_input_opts(src, headers)
    cmd += _reconnect_opts(src)
    cmd += ["-t", str(ANALYZE_MAX_SECONDS), "-i", src]
    cmd += ["-filter_complex", graph] + maps + ["-f", "null", "-"]

    log_path = os.path.join(tmpdir, "ffmpeg.log")
    # Unbuffered: the job reads this log while ffmpeg still runs, to pick the
    # source duration out of the header and turn seconds into a percentage.
    with open(log_path, "wb", buffering=0) as errf:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=errf,
                                cwd=tmpdir, text=True, bufsize=1)
        try:
            for line in proc.stdout:
                if line.startswith("out_time_us="):
                    try:
                        on_progress(int(line.split("=", 1)[1]) / 1e6, log_path)
                    except ValueError:
                        pass
        finally:
            try:
                proc.stdout.close()
            except OSError:
                pass
            proc.wait()
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            log = f.read()
    except OSError:
        log = ""
    return proc.returncode, log


def _log_duration(log_path):
    """Source duration from a partially written ffmpeg log, or 0.0."""
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            m = _DUR_RE.search(f.read(8192))
    except OSError:
        return 0.0
    if not m:
        return 0.0
    return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))


def _analyze_curves(tmpdir):
    """Turn the two metadata logs into one-second curves.

    motion[i] is the mean frame difference over second i, loud[i] the loudest
    momentary loudness in it (LUFS, floored at silence), and cuts the times a
    scene actually changed. One value per second is enough to draw a timeline
    and to pick peaks, and keeps an hour of video under 100 KB of JSON.
    """
    vmeta = _parse_metadata_log(os.path.join(tmpdir, "video.txt"),
                                ("lavfi.scd.mafd", "lavfi.scd.score"))
    mafd = vmeta["lavfi.scd.mafd"]
    scene = vmeta["lavfi.scd.score"]
    ameta = _parse_metadata_log(os.path.join(tmpdir, "audio.txt"),
                                ("lavfi.r128.M",))
    loudness = ameta["lavfi.r128.M"]

    end = 0.0
    for series in (mafd, loudness):
        if series:
            end = max(end, series[-1][0])
    # No samples at all means ffmpeg never decoded a frame (a dead URL, a 403);
    # empty curves are how the caller tells that apart from a silent video.
    n = int(end) + 1 if (mafd or loudness) else 0

    motion = [0.0] * n
    seen = [0] * n
    for when, val in mafd:
        i = int(when)
        if 0 <= i < n:
            motion[i] += val
            seen[i] += 1
    motion = [round(motion[i] / seen[i], 2) if seen[i] else 0.0 for i in range(n)]

    loud = [ANALYZE_SILENCE] * n
    for when, val in loudness:
        i = int(when)
        if 0 <= i < n:
            loud[i] = max(loud[i], max(ANALYZE_SILENCE, val))
    loud = [round(v, 1) for v in loud]

    cuts = sorted({round(when, 2) for when, val in scene if val >= ANALYZE_SCENE_MIN})

    return {
        "step": 1.0,
        "duration": round(end, 2),
        "motion": motion,
        "loud": loud,
        "cuts": cuts,
        "hasAudio": bool(loudness),
    }


def _run_analyze_job(job_id, url, qmax, dur_hint):
    """Background worker: resolve, measure, cache the curves.

    A remote stream that dies halfway leaves curves that look complete and
    describe only the beginning of the video, so the measured length is checked
    against the length ffmpeg reported and a short pass is retried against a
    freshly resolved stream.
    """
    tmpdir = tempfile.mkdtemp(prefix="msanalyze_")
    _set_analyze(job_id, tmpdir=tmpdir)
    try:
        with _analyze_slots:
            def progress(seconds, log_path):
                with _analyze_jobs_lock:
                    job = _analyze_jobs.get(job_id)
                    if not job:
                        return
                    job["at"] = seconds
                    known = job.get("dur") or 0.0
                if not known:
                    found = _log_duration(log_path)
                    if found:
                        _set_analyze(job_id, dur=found)

            best = None
            for attempt in range(2):
                _set_analyze(job_id, stage="resolving")
                try:
                    src, headers, title = _analysis_source(url, qmax, refresh=attempt > 0)
                except Exception as e:  # noqa: BLE001
                    _set_analyze(job_id, stage="error",
                                 error=(str(e).splitlines() or ["resolve failed"])[-1][:250])
                    return

                _set_analyze(job_id, stage="analyzing", at=0.0, dur=float(dur_hint or 0.0))
                rc, log = _analyze_pass(src, headers, tmpdir, True, progress)
                # A video with no audio track fails the filtergraph, not the
                # decode: measure the picture on its own rather than give up.
                if rc != 0 and "matches no streams" in log:
                    rc, log = _analyze_pass(src, headers, tmpdir, False, progress)

                data = _analyze_curves(tmpdir)
                data["title"] = title
                m = _DUR_RE.search(log)
                expect = (int(m.group(1)) * 3600 + int(m.group(2)) * 60
                          + float(m.group(3))) if m else float(dur_hint or 0.0)
                if best is None or data["duration"] > best[0]["duration"]:
                    best = (data, expect, log)
                if data["motion"] and (not expect or data["duration"] >= expect * 0.9):
                    break
                sys.stderr.write("  ANALYZE short read %s: %s of %s (rc=%s)%s\n"
                                 % (url[:60], _hms(data["duration"]), _hms(expect), rc,
                                    " - retrying" if attempt == 0 else ""))

            data, expect, log = best
            if not data["motion"]:
                tail = [l for l in log.strip().splitlines() if l.strip()][-1:] or ["no frames"]
                _set_analyze(job_id, stage="error",
                             error="could not read the video: " + tail[0][:200])
                return
            if expect and data["duration"] < expect * 0.9:
                _set_analyze(job_id, stage="error",
                             error="the stream stopped after %s of %s - measured only "
                                   "the beginning, so nothing was cached. Try again, or "
                                   "Pack the tile first."
                                   % (_hms(data["duration"]), _hms(expect)))
                return

            _analyze_cache_put(url, qmax, data)
            _set_analyze(job_id, stage="done", data=data)
    except Exception as e:  # noqa: BLE001
        _set_analyze(job_id, stage="error", error=str(e)[:200])
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------- /api/clip (say what you are looking for, in words) ----------

# The moments panel finds where something happens; this finds where the thing
# you asked for happens. It is OpenAI's CLIP, run locally through onnxruntime:
# one frame a second is turned into a 512-number vector, your prompt is turned
# into a vector by the same model, and their dot product says how much that
# second looks like those words. No service, no key, no per-request cost —
# just two ONNX files (~600 MB, fetched once) and the CPU or GPU already here.
#
# Optional by design: without numpy/onnxruntime installed, or before the model
# is downloaded, /api/clip/status says so and the panel keeps working on scene
# cuts, motion and loudness alone.

# Bumped whenever the front-end needs something this server did not have. The
# panel checks it and says "restart the server" instead of failing at the first
# call to an endpoint that does not exist yet.
API_VERSION = 3

CLIP_DIR = os.path.join(MS_CACHE_HOME, "clip")
CLIP_INDEX_DIR = os.path.join(MS_CACHE_HOME, "clipidx")
CLIP_HF = "https://huggingface.co/%s/resolve/main/"

# Three sizes of the same idea. B/32 cuts a frame into 32-pixel patches — fast,
# but a small thing in a corner is a fraction of one patch. B/16 quarters the
# patch size for four times the work, and L/14 is the big one: much better at
# "the moment where X happens", several times slower again. Sizes are the
# integrity check — a half written model fails deep inside onnxruntime with an
# unreadable error.
CLIP_MODELS = {
    "b32": {
        "label": "ViT-B/32 · fast",
        "repo": "Xenova/clip-vit-base-patch32",
        "files": {"vision_model.onnx": ("onnx/vision_model.onnx", 351685709),
                  "text_model.onnx": ("onnx/text_model.onnx", 254058553),
                  "tokenizer.json": ("tokenizer.json", 2224119)},
    },
    "b16": {
        "label": "ViT-B/16 · sharper",
        "repo": "Xenova/clip-vit-base-patch16",
        "files": {"vision_model.onnx": ("onnx/vision_model.onnx", 345060583),
                  "text_model.onnx": ("onnx/text_model.onnx", 254058553),
                  "tokenizer.json": ("tokenizer.json", 2224081)},
    },
    "l14": {
        "label": "ViT-L/14 · best",
        "repo": "Xenova/clip-vit-large-patch14",
        "files": {"vision_model.onnx": ("onnx/vision_model.onnx", 1216438437),
                  "text_model.onnx": ("onnx/text_model.onnx", 494947485),
                  "tokenizer.json": ("tokenizer.json", 2224081)},
    },
}
CLIP_DEFAULT_MODEL = "b32"
CLIP_DEFAULT_PRESET = "cumshot"

# A phrase on its own is a weak query: averaging the same phrase through a few
# carriers is CLIP's own prompt-ensembling trick, and it costs four text
# encodes (milliseconds) for a noticeably steadier curve.
CLIP_TEMPLATES = ("{}", "a photo of {}", "a video frame of {}", "a screenshot of {}")

# CLIP distances are not an absolute scale: "a plate of spaghetti" scores ~0.24
# against every frame of a video that has no food in it, and there is no fixed
# number above which a phrase is "present". What IS meaningful is the
# competition — so each second is scored against your phrases AND a set of
# ordinary ones, and what comes back is the share your phrase won. That reads
# as a probability, is comparable between phrases, and lets a search answer
# "nothing here matched" instead of confidently marking the least bad second.
CLIP_BACKGROUND = (
    "a random video frame",
    "an ordinary scene",
    "a black screen",
    "a blurry image",
    "text on a screen",
    "a person standing still",
    "an empty room",
    "a wide landscape",
    "a close-up of an object",
    "a crowd of people",
    "a menu or user interface",
    "static noise",
)

# CLIP looks at 224 px, and the frame is letterboxed into that square before
# the encoder sees it. A 360p rendition already has more height than that, so
# anything larger is bytes downloaded to be thrown away — and downloading is
# most of the wait on a long page.
CLIP_INDEX_QUALITY = 360

# Reading a signed CDN stream straight into ffmpeg is what kept ending an index
# at 0:51 of 14:03: the socket dies, ffmpeg stops, and the frames that did
# arrive look like a whole video. yt-dlp knows how to retry a fragment and how
# to resume, so the file lands complete before the encoder ever sees it.
CLIP_FETCH_WITH_YTDLP = True
# A source that fails in the right rhythm — every response cut short, every
# retry making a little progress — can keep a downloader busy forever. Indexing
# is something a person is waiting on, so it gets a wall clock.
CLIP_FETCH_TIMEOUT = 420
CLIP_FETCH_SOCKET_TIMEOUT = 20

# Indexing downloads the stretch a clip will be cut from and then, until now,
# threw it away — so cutting asked the site for the same seconds all over
# again. Fifty videos indexed and then fifty spans re-requested is what makes a
# site start answering with a challenge page instead of video ("PhantomJS is
# required"), and it is pure waste besides. Keep the file; cut from disk.
SPAN_DIR = os.path.join(tempfile.gettempdir(), "multiscreen_spans")
SPAN_CACHE_BYTES = 8 * 1024 * 1024 * 1024
SPAN_TTL = 6 * 3600


def _span_path(url, window, ext=".mp4"):
    key = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return os.path.join(SPAN_DIR, "%s.%s%s" % (key, _window_tag(window), ext))


def span_prune():
    """Keep the kept material bounded: oldest out first, by age then by size."""
    try:
        files = [(os.path.getmtime(f), os.path.getsize(f), f)
                 for f in glob.glob(os.path.join(SPAN_DIR, "*"))]
    except OSError:
        return
    now = time.time()
    total = 0
    for mtime, size, path in sorted(files, reverse=True):
        if now - mtime > SPAN_TTL or total + size > SPAN_CACHE_BYTES:
            try:
                os.remove(path)
            except OSError:
                pass
        else:
            total += size

# A preset is a scene type worth building a compilation out of: a bank of ways
# to say the thing, and — carrying just as much weight — a bank of the scenes
# that get mistaken for it. A single phrase competing against generic
# alternatives cannot separate the finish of a scene from the rest of the same
# scene; naming the neighbours is what draws that line.
#
# `window` is where the type tends to live: this one closes a video, so the
# search starts at 60% and the front-end offers that by default.
CLIP_PRESETS = {
    "cumshot": {
        "label": "Cumshot compilation",
        "window": [0.6, 1.0],
        # This scene closes a video and runs to the end, so the clip to keep is
        # the LAST strong stretch, not the strongest one. Measured against six
        # videos with known answers, that single rule took the run from two
        # right out of four to three, and made the two videos that must stay
        # quiet quieter (0.34 -> 0.21).
        "prefer": "last",
        # Raised from 0.35 on the evidence of a real sixty-video run: of the 43
        # clips it produced, the weakest 22 sat between 0.35 and 0.60 and were
        # the ones that came back with unrelated footage in them. The cost is
        # measured and real — on the six videos with known answers, the match
        # at 0.41 is lost, so recall goes 3/4 -> 2/4. This preset is for a
        # compilation somebody sells, where a wrong clip costs more than a
        # missing one; the Confidence control moves it back for other uses.
        "gate": 0.6,
        # How far down from its peak a clip may reach. At 0.5 the clips ran 77s
        # and 63s on the known videos and carried unrelated seconds with them;
        # at 0.7 the same moments come out at 20s and 30s with the hit rate
        # untouched (3/4, no false positives).
        "grow": 0.7,
        # Left on the default encoder deliberately. B/16 sees the one scene
        # B/32 is blind to (0.32 where B/32 reads 0.11), but it also invents one
        # in a video that has no such scene at all, and at 0.56 — higher than
        # the true match it rescued, so no threshold separates them. For a
        # compilation, a wrong clip in the export costs more than a missing one.
        "model": "",
        "positive": [
            "a man ejaculating",
            "the moment of ejaculation",
            "semen on a woman's face",
            "cum on her face",
            "cum in her mouth",
            "cum on her tongue",
            "a woman's face covered in semen",
            "semen dripping down her chin",
        ],
        "negative": [
            "a couple having sex",
            "oral sex",
            "a woman undressing",
            "a couple kissing",
            "a woman talking to the camera",
            "a woman posing for the camera",
            "an empty bed",
            "the closing credits of a video",
            "a website logo or watermark",
        ],
    },
}

# Bumped whenever the frames fed to the model change, so old vectors are not
# silently compared against differently-prepared new ones.
CLIP_INDEX_VERSION = "v3"
# One frame a second: CLIP looks at a still, and a second is already finer than
# any clip you would cut by hand.
CLIP_FPS = 1.0
# Frames per forward pass. Bigger batches barely help on CPU and cost memory.
CLIP_BATCH = 16
CLIP_MEAN = (0.48145466, 0.4578275, 0.40821073)
CLIP_STD = (0.26862954, 0.26130258, 0.27577711)
CLIP_CONTEXT = 77

_clip_lock = threading.Lock()
_clip_sessions = {}                 # file name -> onnxruntime session
_clip_provider = ""
_clip_tokenizer = None
_clip_bpe_cache = {}
_clip_text_cache = {}               # prompt -> unit vector
_clip_setup = {"stage": "idle", "at": 0, "total": 0, "error": "", "file": ""}
_clip_jobs = {}
_clip_jobs_lock = threading.Lock()
# One index at a time: the encoder saturates whatever it runs on, and two at
# once only makes both slower.
_clip_slots = threading.BoundedSemaphore(1)

# CLIP's own pre-tokenizer split, with \p{L}/\p{N} written the way Python's re
# spells them ([^\W\d_] is "a letter", \d one digit at a time).
_CLIP_TOK_RE = re.compile(
    r"<\|startoftext\|>|<\|endoftext\|>|'s|'t|'re|'ve|'m|'ll|'d"
    r"|[^\W\d_]+|\d|(?:[^\s\w]|_)+", re.IGNORECASE)


def _clip_deps():
    """(numpy, onnxruntime) or (None, None) — both are optional extras."""
    try:
        import numpy
        import onnxruntime
        return numpy, onnxruntime
    except ImportError:
        return None, None


def clip_model(name):
    """The config for a model id, falling back to the default."""
    return CLIP_MODELS.get(name) or CLIP_MODELS[CLIP_DEFAULT_MODEL]


def clip_model_id(name):
    return name if name in CLIP_MODELS else CLIP_DEFAULT_MODEL


def _clip_model_dir(model):
    return os.path.join(CLIP_DIR, clip_model_id(model))


def _clip_migrate_flat():
    """Move a pre-multi-model download (files straight in clip/) into b32/."""
    target = os.path.join(CLIP_DIR, "b32")
    for name, (_p, size) in CLIP_MODELS["b32"]["files"].items():
        flat = os.path.join(CLIP_DIR, name)
        try:
            if os.path.getsize(flat) != size:
                continue
        except OSError:
            continue
        try:
            os.makedirs(target, exist_ok=True)
            os.replace(flat, os.path.join(target, name))
        except OSError:
            pass


def _clip_missing(model):
    """Model files that are absent or the wrong size."""
    out = []
    folder = _clip_model_dir(model)
    for name, (_path, size) in clip_model(model)["files"].items():
        try:
            if os.path.getsize(os.path.join(folder, name)) != size:
                out.append(name)
        except OSError:
            out.append(name)
    if out and clip_model_id(model) == "b32":
        _clip_migrate_flat()
        return [n for n in out
                if _file_size(os.path.join(folder, n)) != clip_model(model)["files"][n][1]]
    return out


def _file_size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return -1


def _clip_download(model):
    """Fetch one model's files once, reporting progress into _clip_setup."""
    model = clip_model_id(model)
    files = clip_model(model)["files"]
    folder = _clip_model_dir(model)
    missing = _clip_missing(model)
    total = sum(files[n][1] for n in missing)
    _clip_setup.update({"stage": "downloading", "at": 0, "total": total,
                        "error": "", "file": "", "model": model})
    done = 0
    try:
        os.makedirs(folder, exist_ok=True)
        for name in missing:
            path, size = files[name]
            _clip_setup["file"] = name
            tmp = os.path.join(folder, name + ".part")
            req = urllib.request.Request(CLIP_HF % clip_model(model)["repo"] + path,
                                         headers={"User-Agent": DEFAULT_UA})
            with urllib.request.urlopen(req, timeout=60) as resp, open(tmp, "wb") as f:
                while True:
                    chunk = resp.read(1 << 20)
                    if not chunk:
                        break
                    f.write(chunk)
                    done += len(chunk)
                    _clip_setup["at"] = done
            if os.path.getsize(tmp) != size:
                os.remove(tmp)
                raise IOError("%s came down the wrong size" % name)
            os.replace(tmp, os.path.join(folder, name))
        _clip_setup.update({"stage": "done", "file": ""})
    except Exception as e:  # noqa: BLE001
        _clip_setup.update({"stage": "error", "error": str(e)[:200]})


def _clip_session(model, name):
    """Lazily open an ONNX session, on the best provider this install has."""
    model = clip_model_id(model)
    key = model + "/" + name
    with _clip_lock:
        if key in _clip_sessions:
            return _clip_sessions[key]
        _np, ort = _clip_deps()
        if ort is None:
            raise RuntimeError("onnxruntime is not installed")
        if _clip_missing(model):
            raise RuntimeError("the %s model has not been downloaded yet"
                               % clip_model(model)["label"])
        avail = ort.get_available_providers()
        # CUDA and DirectML both offload to the GPU; DirectML needs no CUDA
        # toolkit, which on Windows is usually the shorter road.
        for prov in ("CUDAExecutionProvider", "DmlExecutionProvider", "CPUExecutionProvider"):
            if prov not in avail:
                continue
            try:
                sess = ort.InferenceSession(os.path.join(_clip_model_dir(model), name),
                                            providers=[prov])
            except Exception:  # noqa: BLE001
                continue
            global _clip_provider
            _clip_provider = prov
            _clip_sessions[key] = sess
            return sess
        raise RuntimeError("onnxruntime has no usable execution provider")


# ---- tokenizer (the CLIP BPE, straight from tokenizer.json) ----

def _clip_byte_encoder():
    bs = (list(range(ord("!"), ord("~") + 1)) + list(range(ord("¡"), ord("¬") + 1))
          + list(range(ord("®"), ord("ÿ") + 1)))
    cs = bs[:]
    n = 0
    for b in range(256):
        if b not in bs:
            bs.append(b)
            cs.append(256 + n)
            n += 1
    return {b: chr(c) for b, c in zip(bs, cs)}


def _clip_vocab(model):
    global _clip_tokenizer
    if _clip_tokenizer is None:
        with open(os.path.join(_clip_model_dir(model), "tokenizer.json"),
                  encoding="utf-8") as f:
            tk = json.load(f)
        merges = {}
        for i, m in enumerate(tk["model"]["merges"]):
            pair = tuple(m.split(" ")) if isinstance(m, str) else tuple(m)
            merges[pair] = i
        _clip_tokenizer = (tk["model"]["vocab"], merges, _clip_byte_encoder())
    return _clip_tokenizer


def _clip_bpe(word, merges):
    """Merge a word's characters by learned rank until nothing merges."""
    hit = _clip_bpe_cache.get(word)
    if hit is not None:
        return hit
    parts = list(word)
    parts[-1] += "</w>"
    while len(parts) > 1:
        rank, i = min((merges.get((parts[j], parts[j + 1]), 1 << 30), j)
                      for j in range(len(parts) - 1))
        if rank == 1 << 30:
            break
        parts[i:i + 2] = [parts[i] + parts[i + 1]]
    _clip_bpe_cache[word] = parts
    return parts


def clip_tokenize(text, model=CLIP_DEFAULT_MODEL):
    vocab, merges, enc = _clip_vocab(model)
    text = re.sub(r"\s+", " ", text).strip().lower()
    ids = [vocab["<|startoftext|>"]]
    for tok in _CLIP_TOK_RE.findall(text):
        word = "".join(enc[b] for b in tok.encode("utf-8"))
        for piece in _clip_bpe(word, merges):
            ids.append(vocab.get(piece, vocab["<|endoftext|>"]))
    return ids[:CLIP_CONTEXT - 1] + [vocab["<|endoftext|>"]]


def clip_text_vector(prompt, model=CLIP_DEFAULT_MODEL):
    """Unit vector for one prompt, memoised — prompts repeat across tiles.

    The phrase is encoded through several carrier sentences and the results
    averaged (CLIP's prompt ensembling): a bare noun and "a photo of" that noun
    land in slightly different places, and the mean of both is a steadier
    target than either.
    """
    model = clip_model_id(model)
    key = (model, prompt.strip().lower())
    hit = _clip_text_cache.get(key)
    if hit is not None:
        return hit
    np, _ort = _clip_deps()
    sess = _clip_session(model, "text_model.onnx")
    acc = None
    for tpl in CLIP_TEMPLATES:
        ids = np.array([clip_tokenize(tpl.format(key[1]), model)], dtype=np.int64)
        vec = sess.run(None, {"input_ids": ids})[0][0].astype(np.float32)
        vec /= (float(np.linalg.norm(vec)) + 1e-8)
        acc = vec if acc is None else acc + vec
    acc /= (float(np.linalg.norm(acc)) + 1e-8)
    _clip_text_cache[key] = acc
    return acc


# ---- indexing a video ----

def _window_tag(window):
    """'full' or e.g. 'w67-100' — part of the cache key, since an index that
    only covers the end of a video must never be served as the whole thing."""
    if not window or (window[0] <= 0.001 and window[1] >= 0.999):
        return "full"
    return "w%d-%d" % (round(window[0] * 100), round(window[1] * 100))


def _index_url_forms(url):
    """The spellings of one video address that should share an index.

    The same video arrives written differently depending on who listed it —
    yt-dlp hands back http://, a page's own markup https://, and a hand-pasted
    link may carry either. Hashing the raw string then measures the same video
    two or three times over: an hour of work thrown away because of four
    characters. The first form is the one new indexes are filed under; the rest
    are looked at before deciding something is missing.
    """
    forms = []
    if url.startswith("http://"):
        forms = ["https://" + url[7:], url]
    elif url.startswith("https://"):
        forms = [url, "http://" + url[8:]]
    else:
        forms = [url]
    seen, out = set(), []
    for form in forms:
        if form not in seen:
            seen.add(form)
            out.append(form)
    return out


def _clip_index_path(url, model, window=None):
    # The tile's playback quality is not part of this any more: indexing always
    # fetches the same small rendition, so two tiles of one video share a file.
    key = hashlib.sha1(_index_url_forms(url)[0].encode("utf-8")).hexdigest()
    return os.path.join(CLIP_INDEX_DIR, "%s.%s.%s.%s.npy"
                        % (key, clip_model_id(model), _window_tag(window),
                           CLIP_INDEX_VERSION))


def _clip_index_existing(url, model, window=None):
    """The index file for this video whichever way its address is written."""
    for form in _index_url_forms(url):
        key = hashlib.sha1(form.encode("utf-8")).hexdigest()
        path = os.path.join(CLIP_INDEX_DIR, "%s.%s.%s.%s.npy"
                            % (key, clip_model_id(model), _window_tag(window),
                               CLIP_INDEX_VERSION))
        if os.path.exists(path):
            return path
    return _clip_index_path(url, model, window)


def _clip_meta_path(url, model, window=None):
    return _clip_index_path(url, model, window)[:-4] + ".json"


def _clip_meta_existing(url, model, window=None):
    return _clip_index_existing(url, model, window)[:-4] + ".json"


def _clip_meta_read(url, model, window=None):
    try:
        with open(_clip_meta_existing(url, model, window), encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def clip_index_load(url, model, window=None):
    """The cached vectors for a video, or None if there are none worth using.

    An index cached before the coverage check existed (or by an older build)
    can cover a fraction of the video. When the measured curves know how long
    the video really is, a short index is treated as absent so it is rebuilt —
    silently answering about the first minute of an hour is the worst failure
    this feature has.
    """
    np, _ort = _clip_deps()
    if np is None:
        return None
    try:
        vecs = np.load(_clip_index_existing(url, model, window))
    except (OSError, ValueError):
        return None
    # What the video is really worth: the length written beside the index when
    # it was built, or failing that whatever the measured curves know. Without
    # this, a short index cached in an older session quietly outlives it.
    meta = _clip_meta_read(url, model, window) or {}
    known = meta.get("expect") or 0
    if not known and _window_tag(window) == "full":
        known = (_analyze_cache_get(url, 0) or {}).get("duration") or 0
    if known and len(vecs) / CLIP_FPS < known * 0.9:
        sys.stderr.write("  CLIP index for %s covers %s of %s - reindexing\n"
                         % (url[:60], _hms(len(vecs) / CLIP_FPS), _hms(known)))
        return None
    return vecs


def _clip_frames(src, headers, tmpdir, on_frame, seek=None, take=None):
    """Feed 224x224 RGB frames, one per second, to `on_frame`.

    ffmpeg does the whole CLIP preprocessing itself — sample, scale the short
    side to 224 bicubic, centre crop — and writes raw RGB down a pipe, so no
    image library is involved and nothing hits the disk.
    """
    # Fit the WHOLE frame into the square and pad the rest grey. The obvious
    # alternative, cropping to the centre square, throws away 44% of a 16:9
    # frame — and whatever you are looking for is often exactly there.
    vf = ("fps=%g,scale=224:224:force_original_aspect_ratio=decrease:flags=bicubic,"
          "pad=224:224:(ow-iw)/2:(oh-ih)/2:color=0x727272,format=rgb24" % CLIP_FPS)
    cmd = [FFMPEG, "-hide_banner", "-nostats"]
    if seek:
        cmd += ["-ss", "%.3f" % seek]
    cmd += _ffmpeg_input_opts(src, headers)
    cmd += _reconnect_opts(src)
    cmd += ["-t", "%.3f" % (take or ANALYZE_MAX_SECONDS), "-i", src, "-vf", vf,
            "-an", "-f", "rawvideo", "-"]
    size = 224 * 224 * 3
    # stderr goes to a file, not a pipe: a long remote read can print more than
    # a pipe buffer holds, and a full pipe would deadlock the reader below.
    log_path = os.path.join(tmpdir, "ffmpeg.log")
    with open(log_path, "wb", buffering=0) as errf:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=errf)
        try:
            while True:
                buf = proc.stdout.read(size)
                if not buf or len(buf) < size:
                    break
                on_frame(buf)
        finally:
            try:
                proc.stdout.close()
            except OSError:
                pass
            proc.wait()
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            err = f.read()
    except OSError:
        err = ""
    return proc.returncode, err, _log_duration(log_path)


def _download_for_index(url, tmpdir, span=None):
    """Bring a video (or one span of it) down with yt-dlp, video only.

    Sound plays no part in indexing and a separate audio stream would only add
    a merge step, so this asks for a video-only rendition no taller than the
    encoder can use. Returns a path or None.
    """
    import yt_dlp
    from yt_dlp.utils import download_range_func

    deadline = time.time() + CLIP_FETCH_TIMEOUT

    def watchdog(_status):
        # Raised from inside the download loop, which is the only place able to
        # stop it; yt-dlp has no timeout of its own.
        if time.time() > deadline:
            raise yt_dlp.utils.DownloadError(
                "gave up after %ds - the source is not delivering" % CLIP_FETCH_TIMEOUT)

    q = CLIP_INDEX_QUALITY
    opts = {
        "quiet": True, "no_warnings": True, "noprogress": True, "noplaylist": True,
        "logger": _YdlLogger(),
        "format": ("bv*[height<=%d]/wv*[height<=%d]/best[height<=%d]/best" % (q, q, q)),
        "outtmpl": {"default": os.path.join(tmpdir, "idx.%(ext)s")},
        "retries": 3,
        "fragment_retries": 5,
        "socket_timeout": CLIP_FETCH_SOCKET_TIMEOUT,
        "progress_hooks": [watchdog],
    }
    if span:
        opts["download_ranges"] = download_range_func(None, [span])
    if _IMPERSONATE is not None:
        opts["impersonate"] = _IMPERSONATE
    if FFMPEG_DIR:
        opts["ffmpeg_location"] = FFMPEG_DIR

    with _lock_for_url(url):
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([_canonical_extractor_url(url)])
    for name in sorted(os.listdir(tmpdir)):
        if name.startswith("idx."):
            path = os.path.join(tmpdir, name)
            if os.path.getsize(path) > 0:
                return path
    return None


def _clip_fetch(url, window, dur_hint, tmpdir, refresh):
    """What to hand the encoder, as (source, headers, seek, take, offset, expected, note).

    `offset` is where the returned material starts inside the original video,
    so a curve built from the last third still reports real timestamps.

    A local file is read where it lies. Anything remote is downloaded first —
    reading a signed stream live is what kept truncating these indexes at
    0:51 of 14:03, while yt-dlp retries fragments instead of stopping. Three
    steps, each a fallback for the one before: just the window, then the whole
    file, then the live stream that used to be the only option.
    """
    src, headers, _title = _analysis_source(url, CLIP_INDEX_QUALITY, refresh=refresh)
    local = not str(src).lower().startswith(("http://", "https://"))

    duration = float(dur_hint or 0)
    if not duration and local:
        duration = (probe_media(src) or {}).get("dur") or 0
    if not duration and not local:
        # The extraction that just produced `src` knew the length; it is sitting
        # in the resolve cache, so this costs nothing and saves downloading the
        # whole video because the window could not be worked out.
        try:
            duration = float(resolve_stream(url, CLIP_INDEX_QUALITY).get("duration") or 0)
        except Exception:  # noqa: BLE001
            duration = 0.0

    start, end = 0.0, 0.0
    if window and duration:
        start = max(0.0, duration * float(window[0]))
        end = min(duration, duration * float(window[1]))
        if end - start < 5:
            start, end = 0.0, 0.0
    span_len = (end - start) if end else 0.0

    # When the duration is unknown the window cannot be turned into seconds, so
    # the whole video is read. Saying so keeps it from being filed away as if it
    # were the window that was asked for.
    applied = window if span_len else None

    if local:
        return (src, {}, start or None, span_len or None, start,
                span_len or duration, "local", applied)

    # What this fetch owes the caller, whatever route it takes. Deliberately NOT
    # measured from the file that comes back: a download cut short would then
    # lower the bar it is about to be judged against, and a six second file
    # would pass as a thirty-four second window.
    expect = span_len or duration

    if CLIP_FETCH_WITH_YTDLP:
        for label, span in (("span", (start, end) if end else None), ("full", None)):
            if label == "span" and span is None:
                continue
            try:
                got = _download_for_index(url, tmpdir, span)
            except Exception as e:  # noqa: BLE001
                got = None
                sys.stderr.write("  CLIP %s download failed (%s)\n"
                                 % (label, str(e).splitlines()[-1][:110]))
            if not got:
                continue
            # Move it out of the scratch directory, which is about to be
            # deleted, into the cache the cutting step reads from.
            try:
                os.makedirs(SPAN_DIR, exist_ok=True)
                kept = _span_path(url, window if label == "span" else None,
                                  os.path.splitext(got)[1] or ".mp4")
                os.replace(got, kept)
                got = kept
            except OSError:
                pass
            got_len = (probe_media(got) or {}).get("dur") or 0
            if label == "span":
                # The file is meant to BE the window. If it came back short,
                # say so and let the whole-file route have a go.
                if span_len and got_len < span_len * 0.9:
                    sys.stderr.write("  CLIP span came back %s of %s - fetching it whole\n"
                                     % (_hms(got_len), _hms(span_len)))
                    os.remove(got)
                    continue
                return got, {}, None, None, start, expect, "yt-dlp span", applied
            return (got, {}, start or None, span_len or None, start, expect,
                    "yt-dlp full", applied)

    # Last resort: the live stream, which is what used to truncate.
    return (src, headers, start or None, span_len or None, start, expect,
            "stream", applied)


def _clip_index_once(url, model, on_progress, refresh, window, dur_hint):
    """One indexing attempt. Returns (vectors, seconds covered, expected, log)."""
    np, _ort = _clip_deps()
    sess = _clip_session(model, "vision_model.onnx")
    mean = np.array(CLIP_MEAN, dtype=np.float32)
    std = np.array(CLIP_STD, dtype=np.float32)

    out = []
    batch = []

    def flush():
        if not batch:
            return
        x = np.stack([np.frombuffer(b, dtype=np.uint8).reshape(224, 224, 3) for b in batch])
        x = (x.astype(np.float32) / 255.0 - mean) / std
        x = np.ascontiguousarray(np.transpose(x, (0, 3, 1, 2)))
        vec = sess.run(None, {"pixel_values": x})[0].astype(np.float32)
        vec /= (np.linalg.norm(vec, axis=1, keepdims=True) + 1e-8)
        out.append(vec)
        batch.clear()

    def on_frame(buf):
        batch.append(buf)
        if len(batch) >= CLIP_BATCH:
            flush()
            on_progress(sum(len(v) for v in out))

    tmpdir = tempfile.mkdtemp(prefix="msclip_")
    try:
        (src, headers, seek, take, offset, expect,
         how, applied) = _clip_fetch(url, window, dur_hint, tmpdir, refresh)
        # Where the file that was read begins inside the original video: a span
        # download starts at the window, a whole one at zero.
        kept = src if str(src).startswith(SPAN_DIR) else ""
        kept_start = offset if how == "yt-dlp span" else 0.0
        rc, err, seen = _clip_frames(src, headers, tmpdir, on_frame, seek, take)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    flush()
    vecs = np.concatenate(out) if out else None
    covered = (len(vecs) / CLIP_FPS) if vecs is not None else 0.0
    # `expect` is what was asked for and is the honest bar; `seen` (how long the
    # file ffmpeg opened turned out to be) only stands in when nothing else is
    # known, because a truncated file reports its truncated self.
    want = expect or seen
    return vecs, covered, want, rc, err, offset, how, applied, kept, kept_start


def clip_index_video(url, model, on_progress, dur_hint=0.0, window=None):
    """Embed every sampled frame of `url` and store the result.

    An index that stops early is worse than no index: the search then answers
    confidently about the first thirty seconds of a fourteen minute video. So
    the length actually read is checked against the video's own duration, and a
    short read is retried against a freshly resolved stream (the usual cause is
    a signed CDN URL dying mid-read) before anything is believed or cached.
    """
    np, _ort = _clip_deps()
    best = None
    offset = 0.0
    for attempt in range(2):
        (vecs, covered, expect, rc, err, offset, how, applied,
         kept, kept_start) = _clip_index_once(
            url, model, on_progress, attempt > 0, window, dur_hint)
        window = applied            # file it under what was actually read
        # `best[0] or []` would ask a numpy array whether it is truthy, which
        # raises "the truth value of an array is ambiguous" — and it raised it
        # on the second attempt only, so it hid until a stream read short.
        kept = 0 if best is None or best[0] is None else len(best[0])
        if best is None or (vecs is not None and len(vecs) > kept):
            best = (vecs, covered, expect, err, offset)
        if vecs is not None and (not expect or covered >= expect * 0.9):
            break
        sys.stderr.write("  CLIP short read %s via %s: %s of %s (rc=%s)%s\n"
                         % (url[:50], how, _hms(covered), _hms(expect), rc,
                            " - retrying" if attempt == 0 else ""))

    vecs, covered, expect, err, offset = best
    if vecs is None or not len(vecs):
        tail = [l for l in err.strip().splitlines() if l.strip()][-1:] or ["no frames"]
        raise RuntimeError("could not read the video: " + tail[0][:200])
    if expect and covered < expect * 0.9:
        raise RuntimeError(
            "the stream stopped after %s of %s, so nothing past that point could "
            "be searched. Try again, or Pack the tile so it is read from disk."
            % (_hms(covered), _hms(expect)))

    vecs = vecs.astype(np.float16)
    try:
        os.makedirs(CLIP_INDEX_DIR, exist_ok=True)
        np.save(_clip_index_path(url, model, window), vecs)
        with open(_clip_meta_path(url, model, window), "w", encoding="utf-8") as f:
            json.dump({"count": int(len(vecs)), "expect": round(expect, 2),
                       "offset": round(offset, 2), "fps": CLIP_FPS,
                       "window": _window_tag(window), "file": kept,
                       "file_start": round(kept_start, 2),
                       "model": clip_model_id(model), "ts": int(time.time())}, f)
    except (OSError, ValueError):
        pass
    return vecs


def _set_clip_job(job_id, **fields):
    with _clip_jobs_lock:
        job = _clip_jobs.get(job_id)
        if job:
            job.update(fields)


def _run_clip_job(job_id, url, model, dur_hint=0.0, window=None):
    try:
        with _clip_slots:
            _set_clip_job(job_id, stage="indexing")

            def progress(n):
                _set_clip_job(job_id, at=n)

            vecs = clip_index_video(url, model, progress, dur_hint, window)
            _set_clip_job(job_id, stage="done", at=len(vecs), count=len(vecs))
    except Exception as e:  # noqa: BLE001
        _set_clip_job(job_id, stage="error",
                      error=(str(e).splitlines() or ["indexing failed"])[-1][:250])


# How many of the example's frames a second is allowed to be judged by. One
# would let a single odd frame decide; the average of the best three is steady
# without blurring different clips together.
CLIP_LIKE_TOPK = 3


def clip_like(vecs, ref, span=None):
    """Per-second likeness to an example clip, in 0..1, plus a quality note.

    Words are a poor way to ask for "this exact kind of moment" — you already
    have one, in another tile. Each second is compared against **every frame**
    of the example and keeps its best few matches; averaging the example into
    one query vector instead (what this did first) blurs a compilation of
    several different clips into a smear that resembles none of them. Measured
    on real footage, the per-frame form picks seconds that are markedly more
    alike, both to the example and to each other.

    The scale is then set by the video itself — its own median second is 0 and
    its best is 1 — because raw CLIP distance saturates on footage that shares
    a performer and a room: everything sits between 0.85 and 0.96, and an
    absolute threshold there means nothing. `gap` reports how much room there
    was between the ordinary second and the best one, which is the honest
    measure of whether the model could tell them apart at all.
    """
    np, _ort = _clip_deps()
    ref = ref.astype(np.float32)
    if span:
        a = max(0, int(span[0]))
        b = min(len(ref), int(span[1]) + 1)
        if b - a >= 1:
            ref = ref[a:b]

    frames = vecs.astype(np.float32)
    sim = frames @ ref.T
    k = min(CLIP_LIKE_TOPK, sim.shape[1])
    raw = np.sort(sim, axis=1)[:, -k:].mean(axis=1)

    median = float(np.median(raw))
    peak = float(np.percentile(raw, 99.5))
    gap = peak - median
    curve = np.clip((raw - median) / max(gap, 1e-6), 0.0, 1.0)
    return ([round(float(v), 4) for v in curve],
            {"gap": round(gap, 4), "peak": round(float(raw.max()), 4),
             "median": round(median, 4)})


def clip_pick_all(curve, offset=0.0, duration=0.0, gate=0.5, grow=0.7,
                  min_len=8.0, max_len=96.0, margin=3.0, budget=0.0, gap=3,
                  limit=200):
    """Every stretch worth keeping, strongest first.

    `clip_pick_moment` answers "does this video have the thing, and where" — one
    clip, because an ordinary video has the thing once. A compilation has it
    twenty times, and re-cutting one is a different question: which minutes are
    the strongest, and how many fit in the length being aimed at.

    With a `budget` in seconds, the clips are chosen by confidence until the
    budget is met and then handed back in time order — so a ten minute target
    keeps the ten best minutes, not the first ten.
    """
    if not curve:
        return []
    smooth = _smooth(curve)
    n = len(smooth)
    used = bytearray(n)
    found = []

    while len(found) < limit:
        seed, peak = -1, -1.0
        for i in range(n):
            if not used[i] and smooth[i] > peak:
                seed, peak = i, smooth[i]
        if seed < 0 or peak < gate:
            break

        floor = max(peak * grow, 0.05)
        a = b = seed
        while a > 0 and not used[a - 1] and smooth[a - 1] >= floor and (b - a + 1) < max_len:
            a -= 1
        while b < n - 1 and not used[b + 1] and smooth[b + 1] >= floor and (b - a + 1) < max_len:
            b += 1

        lo, hi = float(a), float(b + 1)
        if hi - lo < min_len:
            lo = max(0.0, lo - (min_len - (hi - lo)) / 2.0)
            hi = min(float(n), lo + min_len)
            lo = max(0.0, hi - min_len)

        for i in range(max(0, int(lo) - gap), min(n, int(hi) + gap)):
            used[i] = 1

        start = max(0.0, offset + lo - margin)
        end = min(duration or (offset + n), offset + hi + margin)
        if end > start + 0.5:
            found.append({"start": round(start, 2), "end": round(end, 2),
                          "score": round(float(peak), 4), "at": round(offset + seed, 2)})

    found.sort(key=lambda m: -m["score"])
    if budget:
        kept, total = [], 0.0
        for m in found:
            if total >= budget:
                break
            kept.append(m)
            total += m["end"] - m["start"]
        found = kept
    return sorted(found, key=lambda m: m["start"])


def clip_classify(vecs, preset, model):
    """Per-second odds that a second is this preset's scene type, in 0..1.

    The positive phrasings are averaged into one prototype — several ways of
    saying a thing land in slightly different places, and their mean is a
    steadier target than any of them. Every second is then a softmax between
    that prototype, the scenes known to be mistaken for it, and the ordinary
    background. What comes back is the share the prototype won, which is a
    probability rather than a distance, and so comparable between videos.
    """
    np, _ort = _clip_deps()
    cfg = CLIP_PRESETS[preset]

    pos = None
    for phrase in cfg["positive"]:
        v = clip_text_vector(phrase, model)
        pos = v.copy() if pos is None else pos + v
    pos /= (float(np.linalg.norm(pos)) + 1e-8)

    against = [clip_text_vector(n, model) for n in cfg["negative"]]
    against += [clip_text_vector(b, model) for b in CLIP_BACKGROUND]

    frames = vecs.astype(np.float32)
    logits = 100.0 * (frames @ np.stack([pos] + against).T)
    logits -= logits.max(axis=1, keepdims=True)
    exp = np.exp(logits)
    probs = exp / (exp.sum(axis=1, keepdims=True) + 1e-9)
    return [round(float(v), 4) for v in probs[:, 0]]


def _smooth(curve, radius=2):
    """Centred moving average, so one odd second cannot place a cut."""
    n = len(curve)
    pre = [0.0]
    for v in curve:
        pre.append(pre[-1] + v)
    out = []
    for i in range(n):
        a, b = max(0, i - radius), min(n, i + radius + 1)
        out.append((pre[b] - pre[a]) / (b - a))
    return out


def clip_pick_moment(curve, offset=0.0, duration=0.0, gate=0.35, prefer="last",
                     min_len=8.0, max_len=96.0, margin=5.0, grow=0.5):
    """The one clip worth keeping from a scored video, or None.

    This is the picking that was calibrated against six videos with known
    answers, moved here so a compilation can be built without a browser:

    * `prefer="last"` takes the last stretch still scoring near the top rather
      than the single highest second — a scene that closes a video is the last
      thing that looks like it, and something earlier often looks more like the
      words than the real thing does (that one rule: 2/4 right -> 3/4).
    * the clip then grows out to half of its own peak, which is where the scene
      ends rather than where a fixed length happens to fall,
    * and a margin keeps a little air on both sides.

    Times come back on the video's own clock, `offset` being where the scored
    stretch starts inside it.
    """
    if not curve:
        return None
    smooth = _smooth(curve)
    n = len(smooth)
    top = max(smooth)
    if top <= 0:
        return None

    if prefer == "last":
        bar = top * 0.5
        end = max((i for i in range(n) if smooth[i] >= bar), default=-1)
        if end < 0:
            return None
        start = end
        while start > 0 and smooth[start - 1] >= bar:
            start -= 1
        seed = max(range(start, end + 1), key=lambda i: smooth[i])
    else:
        seed = max(range(n), key=lambda i: smooth[i])

    peak = smooth[seed]
    if peak < gate:
        return None

    # How far down the peak the clip is allowed to reach. Lower keeps the whole
    # scene and lets unrelated seconds ride along; higher keeps only the part
    # that clearly is the thing.
    floor = max(peak * grow, 0.05)
    a = b = seed
    while a > 0 and smooth[a - 1] >= floor and (b - a + 1) < max_len:
        a -= 1
    while b < n - 1 and smooth[b + 1] >= floor and (b - a + 1) < max_len:
        b += 1

    lo, hi = float(a), float(b + 1)
    if hi - lo < min_len:                     # too short: centre a minimum clip
        lo = max(0.0, lo - (min_len - (hi - lo)) / 2.0)
        hi = min(float(n), lo + min_len)
        lo = max(0.0, hi - min_len)

    start = offset + lo - margin
    end = offset + hi + margin
    limit = duration or (offset + n)
    start = max(0.0, start)
    end = min(limit, end)
    if end <= start + 0.5:
        return None
    return {"start": round(start, 2), "end": round(end, 2),
            "score": round(float(peak), 4), "at": round(offset + seed, 2)}


def clip_search(vecs, prompts, model):
    """Per-second, per-prompt share of the match, in 0..1.

    Softmax over [your phrases + CLIP_BACKGROUND] at CLIP's own logit scale of
    100. A second where your words describe the picture better than any of the
    ordinary alternatives lands near 1; a phrase that is simply not in the
    video stays near zero everywhere, which is the answer we want to be able
    to give.
    """
    np, _ort = _clip_deps()
    frames = vecs.astype(np.float32)
    mat = np.stack([clip_text_vector(p, model) for p in prompts]
                   + [clip_text_vector(b, model) for b in CLIP_BACKGROUND])
    logits = 100.0 * (frames @ mat.T)
    logits -= logits.max(axis=1, keepdims=True)
    exp = np.exp(logits)
    probs = exp / (exp.sum(axis=1, keepdims=True) + 1e-9)
    return {p: [round(float(v), 4) for v in probs[:, i]] for i, p in enumerate(prompts)}


# ---------- /api/channel (a whole performer's page, in one go) ----------

# The wall was never the point of a compilation — it was just where the URLs
# happened to be. Fifty tiles playing at once is what makes a browser crawl,
# and none of it is needed: given a model page, yt-dlp lists the videos, the
# preset finds the moment in each, and ffmpeg joins them. No player involved.

CHANNEL_MAX = 200
CHANNEL_JOB_TTL = 7200

_channel_jobs = {}
_channel_jobs_lock = threading.Lock()


def _set_channel(job_id, **fields):
    with _channel_jobs_lock:
        job = _channel_jobs.get(job_id)
        if job:
            job.update(fields)


# A listing page carries its links in plain HTML; that is all this needs.
_VIEWKEY_RE = re.compile(
    r'href="(?:https?://[^"]*)?/view_video\.php\?viewkey=([A-Za-z0-9]+)"'
    r'(?:[^>]*?\stitle="([^"]*)")?', re.I)


def _channel_scrape(url, limit):
    """The listing read straight off the page, for when the extractor is refused.

    Sites answer a second kind of request with a JS challenge ("PhantomJS not
    found") long before they stop serving HTML to something that looks like a
    browser — and the project already has a fetcher that looks like one.
    """
    base = url.rstrip("/")
    if "/model/" in base and not base.endswith("/videos"):
        base += "/videos"

    out, seen = [], set()
    for page in range(1, 6):
        target = base if page == 1 else "%s?page=%d" % (base, page)
        try:
            html, _imp = _fetch_page_html(target)
        except Exception:  # noqa: BLE001
            break
        if not html:
            break
        fresh = 0
        for key, title in _VIEWKEY_RE.findall(html):
            if key in seen:
                continue
            seen.add(key)
            fresh += 1
            out.append({"url": "https://www.pornhub.com/view_video.php?viewkey=" + key,
                        "title": (title or key).strip(), "duration": 0.0})
            if len(out) >= limit:
                return out
        if not fresh:
            break
    return out


def channel_videos(url, limit=CHANNEL_MAX):
    """Every video on a performer/channel page, without opening any of them.

    `extract_flat` stops at the listing, so this is one page fetch and a second
    of work for sixty videos rather than sixty extractions. When the extractor
    is turned away, the page's own HTML still has the links.
    """
    import yt_dlp

    limit = max(1, min(int(limit or CHANNEL_MAX), CHANNEL_MAX))
    opts = {
        "quiet": True, "no_warnings": True, "noprogress": True,
        "extract_flat": "in_playlist",
        "playlistend": limit,
        "logger": _YdlLogger(),
    }
    if _IMPERSONATE is not None:
        opts["impersonate"] = _IMPERSONATE

    title, out, seen = url, [], set()
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
        title = info.get("title") or info.get("id") or url
        for entry in (info.get("entries") or []):
            if not entry:
                continue
            link = entry.get("url") or entry.get("webpage_url") or ""
            if not link or link in seen:
                continue
            seen.add(link)
            out.append({"url": link,
                        "title": entry.get("title") or link,
                        "duration": float(entry.get("duration") or 0)})
    except Exception as e:  # noqa: BLE001
        sys.stderr.write("  CHANNEL extractor refused (%s) - reading the page instead\n"
                         % str(e).splitlines()[-1][:110])

    if not out:
        out = _channel_scrape(url, limit)
        if out:
            sys.stderr.write("  CHANNEL read %d videos off the page itself\n" % len(out))
    return {"title": title, "videos": out[:limit]}


def _channel_cut_for(video, preset, model, on_progress, gate=None):
    """Index one video and return the clip the preset wants, or None."""
    cfg = CLIP_PRESETS[preset]
    window = tuple(cfg["window"])
    vecs = clip_index_load(video["url"], model, window)
    if vecs is None:
        vecs = clip_index_video(video["url"], model, on_progress,
                                video.get("duration") or 0.0, window)
    meta = _clip_meta_read(video["url"], model, window) or {}
    offset = float(meta.get("offset") or 0.0)
    duration = float(video.get("duration") or 0.0) or (offset + len(vecs))

    curve = clip_classify(vecs, preset, model)
    moment = clip_pick_moment(curve, offset=offset, duration=duration,
                              gate=gate if gate is not None else cfg.get("gate", 0.35),
                              prefer=cfg.get("prefer", "best"),
                              grow=cfg.get("grow", 0.5))
    if not moment:
        return None

    # Prefer the file indexing already downloaded: cutting from disk asks the
    # site nothing, which is both faster and the reason this stopped failing
    # halfway through a long run.
    source, start, end = video["url"], moment["start"], moment["end"]
    local = meta.get("file") or ""
    if local and os.path.isfile(local):
        shift = float(meta.get("file_start") or 0.0)
        source = local
        start, end = max(0.0, moment["start"] - shift), moment["end"] - shift

    return {"url": source, "page": video["url"], "title": video["title"],
            "start": round(start, 2), "end": round(end, 2),
            "score": moment["score"], "at": moment["at"], "local": bool(local)}


def _run_channel_job(job_id, url, limit, preset, model, resolution, gate=None):
    """List, measure, pick, cut, join — the whole compilation, server side."""
    try:
        span_prune()
        _set_channel(job_id, stage="listing")
        listing = channel_videos(url, limit)
        videos = listing["videos"]
        if not videos:
            _set_channel(job_id, stage="error", error="no videos found on that page")
            return
        _set_channel(job_id, stage="indexing", title=listing["title"],
                     total=len(videos), done=0, found=0)

        cuts, missed = [], []
        for i, video in enumerate(videos):
            _set_channel(job_id, done=i, current=video["title"][:80])

            def progress(frames, i=i):
                _set_channel(job_id, frames=frames)

            try:
                cut = _channel_cut_for(video, preset, model, progress, gate)
            except Exception as e:  # noqa: BLE001
                missed.append("%s: %s" % (video["title"][:40],
                                          str(e).splitlines()[-1][:90]))
                continue
            if cut:
                cuts.append(cut)
                with _channel_jobs_lock:
                    job = _channel_jobs.get(job_id)
                    if job:
                        job["found"] = len(cuts)
                        job["clips"] = [{"title": c["title"][:70], "start": c["start"],
                                         "end": c["end"], "score": c["score"]}
                                        for c in cuts]
        _set_channel(job_id, done=len(videos), missed=missed[:10])

        if not cuts:
            _set_channel(job_id, stage="error",
                         error="none of the %d videos had the moment "
                               "(or none could be read)" % len(videos))
            return

        # Hand the collected clips to the compile machinery already in place.
        sub_id = uuid.uuid4().hex
        with _compile_jobs_lock:
            _compile_jobs[sub_id] = {"stage": "queued", "done": 0, "total": len(cuts),
                                     "error": None, "result": None, "skipped": 0,
                                     "tmpdir": None, "ts": time.time()}
        _set_channel(job_id, stage="cutting", compile_id=sub_id, total_cuts=len(cuts))

        watcher = threading.Thread(
            target=_run_compile_job,
            args=(sub_id, [{"url": c["url"], "start": c["start"], "end": c["end"]}
                           for c in cuts],
                  COMPILE_RES.get(str(resolution), COMPILE_RES["720"]),
                  int(resolution) if str(resolution).isdigit() else 720),
            daemon=True)
        watcher.start()
        while watcher.is_alive():
            with _compile_jobs_lock:
                sub = dict(_compile_jobs.get(sub_id) or {})
            _set_channel(job_id, cut_done=sub.get("done", 0),
                         stage="joining" if sub.get("stage") == "concatenating" else "cutting")
            time.sleep(0.5)

        with _compile_jobs_lock:
            sub = dict(_compile_jobs.get(sub_id) or {})
        if sub.get("stage") != "done":
            _set_channel(job_id, stage="error",
                         error=sub.get("error") or "the clips could not be joined")
            return
        _set_channel(job_id, stage="done", result=sub.get("result"),
                     skipped=sub.get("skipped", 0), compile_id=sub_id)
    except Exception as e:  # noqa: BLE001
        _set_channel(job_id, stage="error", error=str(e).splitlines()[-1][:250])


# ---------- /api/tts (neural voices for Trance) ----------

# The Web Speech API only offers what the machine has installed — on a fresh
# Windows that is Zira and David, and Chrome shows no Microsoft Natural voice
# at all. edge-tts talks to the same free endpoint Edge's Read Aloud uses, so
# any browser gets ~160 neural female voices, with the gender declared by the
# service instead of guessed from the first name.
#
# Optional: without edge-tts installed the endpoints answer 503 and the page
# falls back to the browser's own voices.

TTS_MAX_CHARS = 600
TTS_CACHE_MAX = 240                 # phrases loop, so a small LRU covers a session
TTS_CACHE_BYTES = 48 * 1024 * 1024
TTS_VOICES_TTL = 3600

_tts_voices = None
_tts_voices_at = 0.0
_tts_voices_lock = threading.Lock()
_tts_cache = collections.OrderedDict()  # key -> mp3 bytes
_tts_cache_bytes = 0
_tts_cache_lock = threading.Lock()
_tts_render_locks = {}                  # key -> lock (two tabs, same phrase)


def _tts_label(short):
    """"en-US-AvaMultilingualNeural" -> "Ava Multilingual"."""
    stem = short.rsplit("-", 1)[-1]
    stem = re.sub(r"Neural$", "", stem)
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", stem) or short


def tts_voices():
    """The whole neural roster, fetched once an hour. Raises ImportError."""
    global _tts_voices, _tts_voices_at
    with _tts_voices_lock:
        if _tts_voices and time.time() - _tts_voices_at < TTS_VOICES_TTL:
            return _tts_voices
        import asyncio
        import edge_tts
        raw = asyncio.run(edge_tts.list_voices())
        out = []
        for v in raw:
            short = v.get("ShortName") or ""
            tag = v.get("VoiceTag") or {}
            out.append({
                "name": short,
                "label": _tts_label(short),
                "locale": v.get("Locale") or "",
                "localeName": v.get("LocaleName") or "",
                "gender": (v.get("Gender") or "").lower(),
                "traits": list(tag.get("VoicePersonalities") or [])[:3],
                "multilingual": "Multilingual" in short,
            })
        out.sort(key=lambda v: (v["localeName"], v["label"]))
        _tts_voices = out
        _tts_voices_at = time.time()
        return out


def tts_voices_for(lang, gender=""):
    """Voices for a language: exact locale first, then the multilingual ones
    (they speak anything), then the rest of the same language."""
    voices = tts_voices()
    if gender in ("female", "male"):
        voices = [v for v in voices if v["gender"] == gender]
    if not lang:
        return voices
    want = lang.replace("_", "-").lower()
    base = want.split("-")[0]
    exact = [v for v in voices if v["locale"].lower() == want]
    taken = set(v["name"] for v in exact)
    multi = [v for v in voices if v["multilingual"] and v["name"] not in taken]
    taken |= set(v["name"] for v in multi)
    same = [v for v in voices
            if v["locale"].lower().split("-")[0] == base and v["name"] not in taken]
    return exact + multi + same


def _tts_cache_put(key, data):
    global _tts_cache_bytes
    with _tts_cache_lock:
        _tts_cache[key] = data
        _tts_cache_bytes += len(data)
        while _tts_cache and (len(_tts_cache) > TTS_CACHE_MAX
                              or _tts_cache_bytes > TTS_CACHE_BYTES):
            _, old = _tts_cache.popitem(last=False)
            _tts_cache_bytes -= len(old)


def _tts_synth(text, voice, rate, pitch):
    import asyncio
    import edge_tts

    async def run():
        com = edge_tts.Communicate(text, voice, rate=rate, pitch=pitch)
        buf = bytearray()
        async for chunk in com.stream():
            if chunk.get("type") == "audio" and chunk.get("data"):
                buf += chunk["data"]
        return bytes(buf)

    return asyncio.run(run())


def tts_render(text, voice, rate, pitch):
    """MP3 bytes for one phrase, memoised — the loop says the same lines all
    evening, so nothing after the first pass touches the network."""
    key = hashlib.sha1(
        ("%s|%s|%s|%s" % (voice, rate, pitch, text)).encode("utf-8")).hexdigest()
    with _tts_cache_lock:
        hit = _tts_cache.get(key)
        if hit is not None:
            _tts_cache.move_to_end(key)
            return hit
        lock = _tts_render_locks.setdefault(key, threading.Lock())
    with lock:
        with _tts_cache_lock:
            hit = _tts_cache.get(key)
            if hit is not None:
                _tts_cache.move_to_end(key)
                return hit
        data = _tts_synth(text, voice, rate, pitch)
        if not data:
            raise ResolveError("the voice service returned no audio")
        _tts_cache_put(key, data)
        with _tts_cache_lock:
            _tts_render_locks.pop(key, None)
        return data


def _tts_pct(v):
    """Rate/volume as the service wants it: "-25%", clamped and signed."""
    m = re.match(r"^([+-]?\d{1,3})%?$", (v or "").strip())
    n = int(m.group(1)) if m else 0
    n = max(-90, min(200, n))
    return "%+d%%" % n


def _tts_hz(v):
    m = re.match(r"^([+-]?\d{1,3})\s*Hz$", (v or "").strip(), re.I)
    n = int(m.group(1)) if m else 0
    n = max(-100, min(100, n))
    return "%+dHz" % n


# ---------- /api/related (one more tile like the ones already up) ----------

# No site offers a cross-site "related videos" API, but every watch page
# already links to that site's own recommendations. So the wall itself is the
# query: fetch a page one of the tiles came from, keep the links shaped like
# that page, and rank them by how much their words overlap the titles already
# on screen.
#
# "Shaped like" is what separates a video page from a tag, category or profile
# page without knowing anything about the site: the path template. On
# rule34video a watch URL is /video/<digits>/<slug>/, so a link with that exact
# template is another video and /tags/<name>/ is not — and the same trick works
# on a site nobody wrote a rule for.

RELATED_SEED_FETCHES = 3       # pages per request; each one is a real round trip
RELATED_TAG_FETCHES = 2        # index pages for the wall's recurring labels
RELATED_POOL_TTL = 600         # how long a scraped page's links stay usable
RELATED_FAIL_TTL = 120         # ...and how long a page that failed is left alone
RELATED_MAX_CANDIDATES = 400   # video links kept per page
RELATED_MAX_TAGS = 80          # taxonomy links kept per page

# Path segments that mark a site's own taxonomy — the label a video was filed
# under. This is the wall's theme stated by the site instead of guessed from a
# title: the performer, the character, the game, the studio. A page filed under
# the same label is "more of what is on screen" in a way word overlap can only
# approximate.
_TAXONOMY_SEGS = frozenset("""
tag tags category categories cat model models actress actresses star stars
pornstar pornstars performer performers artist artists author authors character
characters game games franchise series studio studios channel channels genre
genres keyword keywords playlist playlists member members user users uploader
tagged label labels topic topics collection collections
""".split())

_related_pool = {}             # seed url -> (expiry, [{url, title}])
_related_lock = threading.Lock()

_A_TAG_RE = re.compile(r"<a\s([^>]*?)>(.*?)</a>", re.I | re.S)
_HREF_RE = re.compile(r"""href\s*=\s*(?:"([^"]*)"|'([^']*)')""", re.I)
_LABEL_ATTR_RE = re.compile(
    r"""(?:title|alt|aria-label)\s*=\s*(?:"([^"]*)"|'([^']*)')""", re.I)
_TAG_STRIP_RE = re.compile(r"<[^>]+>")
_WORD_RE = re.compile(r"[a-z0-9]{2,}")
_NON_PAGE_RE = re.compile(
    r"\.(mp4|webm|m3u8|ts|jpg|jpeg|png|gif|webp|svg|css|js|json|zip|rar|pdf)($|\?)",
    re.I)

# Words that say nothing about *which* video this is.
_RELATED_STOP = frozenset("""
video videos vid clip clips watch free hd sd full new newest best top porn xxx
sex part scene fps 4k 60fps 2160p 1080p 720p 480p 360p com www net org online
download stream streaming mp4 webm the and for with from that this out are was
were her his she him you your our all any not but has have had
to at in on by or of it is as an be do no so up we he my me us if
""".split())


# Query parameters that never say *which* video a link points at.
_TRACKING_PARAMS = frozenset("""
from ref referer referrer src source campaign fbclid gclid msclkid
utm_source utm_medium utm_campaign utm_term utm_content
""".split())


def _norm_url(u):
    """Identity for de-duplication: no scheme, no 'www.', no trailing slash,
    and no tracking parameters — but the identifying ones are kept, because on
    plenty of sites (?v=, ?id=) the query IS the video."""
    try:
        p = urllib.parse.urlsplit((u or "").strip())
    except ValueError:
        return (u or "").strip().lower()
    host = p.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    keep = sorted((k, v) for k, v in
                  urllib.parse.parse_qsl(p.query, keep_blank_values=True)
                  if k.lower() not in _TRACKING_PARAMS)
    tail = "?" + urllib.parse.urlencode(keep) if keep else ""
    return (host + p.path.rstrip("/") + tail).lower()


def _url_shape(url):
    """(host, path template). Digit runs become '#', slugs become '*', and
    short words stay themselves — those are the site's structure ('video',
    'watch', 'v'), which is exactly what must match."""
    try:
        p = urllib.parse.urlsplit(url)
    except ValueError:
        return ("", ())
    segs = []
    for s in [x for x in p.path.split("/") if x]:
        # A colon marks a namespace, not a title ("Category:Foo", "Talk:Foo").
        # Keeping the prefix in the shape is what stops those from passing as
        # articles/videos, since a real one has no prefix at all.
        prefix = ""
        if ":" in s:
            head, _, s = s.partition(":")
            prefix = head.lower() + ":"
        if s.isdigit():
            segs.append(prefix + "#")
        elif len(s) > 18 or "-" in s or "_" in s or any(c.isdigit() for c in s):
            segs.append(prefix + "*")
        else:
            segs.append(prefix + s.lower())
    return (p.netloc.lower(), tuple(segs))


def _words(text):
    return {w for w in _WORD_RE.findall((text or "").lower())
            if w not in _RELATED_STOP and not w.isdigit()}


def _slug_words(url):
    """A URL's own words. On these sites the slug carries the title, so this
    works even when the link had no text at all (thumbnail-only markup)."""
    try:
        path = urllib.parse.urlsplit(url).path
    except ValueError:
        return set()
    return _words(re.sub(r"[-_/.]+", " ", urllib.parse.unquote(path)))


def _is_taxonomy_path(url):
    """The label a taxonomy link names, or "". /tags/shadowheart/ names
    "shadowheart"; /video/123/slug/ names nothing. The marker may sit anywhere
    but last (/en/tags/x, /video/tags/x), and the last segment is the label."""
    try:
        segs = [x for x in urllib.parse.urlsplit(url).path.split("/") if x]
    except ValueError:
        return "", ""
    if len(segs) < 2:
        return "", ""
    low = [x.lower() for x in segs]
    if low[-1] in _TAXONOMY_SEGS:
        return "", ""            # the index itself, not one label
    kind = next((x for x in low[:-1] if x in _TAXONOMY_SEGS), "")
    if not kind:
        return "", ""
    return kind, _pretty_slug(url)


def _mine_page(page_url, text, want_shape):
    """Everything a page offers: links shaped like `want_shape` (other video
    pages) and the site's taxonomy links (what this video is filed under).

    `want_shape` is a parameter rather than derived from `page_url` so a tag
    index can be mined for videos as well — its own shape is a tag's, not a
    video's, and that is exactly the page where every link is on-theme."""
    import html as _html
    videos, tags = {}, {}
    for m in _A_TAG_RE.finditer(text):
        attrs, inner = m.group(1), m.group(2)
        hm = _HREF_RE.search(attrs)
        if not hm:
            continue
        href = (hm.group(1) or hm.group(2) or "").strip()
        if not href or href[0] in "#?" or href.lower().startswith(
                ("javascript:", "mailto:", "data:")):
            continue
        url = urllib.parse.urljoin(page_url, _html.unescape(href)).split("#")[0]
        if not url.lower().startswith(("http://", "https://")):
            continue
        if _NON_PAGE_RE.search(url):
            continue
        key = _norm_url(url)
        lm = _LABEL_ATTR_RE.search(attrs) or _LABEL_ATTR_RE.search(inner)
        label = (lm.group(1) or lm.group(2)) if lm else _TAG_STRIP_RE.sub(" ", inner)
        label = re.sub(r"\s+", " ", _html.unescape(label or "")).strip()[:200]
        # A label of "x", ">>", "HD" or "Watch now" says less than the slug the
        # URL already carries, so it is only kept when it says something.
        if len(label) < 4 or not _words(label):
            label = ""

        if want_shape[1] and _url_shape(url) == want_shape:
            if key not in videos and len(videos) < RELATED_MAX_CANDIDATES:
                videos[key] = {"url": url, "title": label}
            continue
        kind, name = _is_taxonomy_path(url)
        if kind and key not in tags and len(tags) < RELATED_MAX_TAGS:
            tags[key] = {"url": url, "kind": kind, "label": label or name,
                         "name": name}
    return list(videos.values()), list(tags.values())


def _related_pooled(page_url, want_shape):
    """What the pool already holds for this page, or None. Lets a request tell
    free pages from ones that would cost a round trip."""
    now = time.time()
    with _related_lock:
        hit = _related_pool.get((page_url, want_shape))
    return hit[1] if hit and hit[0] > now else None


def _pretty_slug(url):
    """A readable title out of a URL, for links whose markup carried no text
    at all (a bare thumbnail is common in a related-videos grid)."""
    try:
        segs = [x for x in urllib.parse.urlsplit(url).path.split("/") if x]
    except ValueError:
        return url
    for seg in reversed(segs):
        if seg.isdigit():
            continue
        name = re.sub(r"\.[a-z0-9]{2,5}$", "", urllib.parse.unquote(seg), flags=re.I)
        name = re.sub(r"[-_+]+", " ", name).strip()
        if name:
            return name[:120]
    return url


def _mine_cached(page_url, want_shape):
    """_mine_page over the network, memoised. Returns ((videos, tags), cached).

    The cache is what makes repeated clicks cheap: the first pays for the page,
    the rest draw from the pool it filled — and since what is already on the
    wall is excluded per request, each click still yields something new."""
    now = time.time()
    key = (page_url, want_shape)
    with _related_lock:
        hit = _related_pool.get(key)
        if hit and hit[0] > now:
            return hit[1], True
    try:
        text, _used_imp = _fetch_page_html(page_url)
    except Exception:
        # Remember the failure, briefly. Without this a label page that 403s is
        # re-fetched on every single click — twice over, since a refused fetch
        # also costs the impersonation retry underneath.
        with _related_lock:
            _related_pool[key] = (now + RELATED_FAIL_TTL, ([], []))
        raise
    mined = _mine_page(page_url, text, want_shape)
    with _related_lock:
        _related_pool[key] = (now + RELATED_POOL_TTL, mined)
        for dead in [k for k, v in _related_pool.items() if v[0] <= now]:
            _related_pool.pop(dead, None)
    return mined, False


def _bigrams(text):
    """Adjacent word pairs, skipping filler. "Shadowheart Duality" is a much
    stronger match than "shadowheart" and "duality" landing separately."""
    toks = _WORD_RE.findall((text or "").lower())
    out = set()
    for a, b in zip(toks, toks[1:]):
        if a in _RELATED_STOP or b in _RELATED_STOP or a.isdigit() or b.isdigit():
            continue
        out.add(a + " " + b)
    return out


# ---------- /api/find (type a name, get the web's thumbnails) ----------

# Every other feature here starts from a URL you already have. This one starts
# from a name. A web search picks the pages, and each page is mined for its
# thumbnail grid — no list of sites, no per-site rules: an <img> inside an <a>
# is what makes a card, and the link's shape (the same trick Related uses)
# tells a video from a gallery.
#
# Two things keep it from feeling dead while it works: the pages are fetched by
# a pool and the browser polls for whatever has landed, so the first cards show
# up in a couple of seconds; and every mined page is written to disk, so the
# same name searched again paints immediately and costs the sites nothing.

FIND_JOB_TTL = 900             # a finished search stays fetchable this long
FIND_PAGES = 30                # pages mined per search — each one a round trip
FIND_WORKERS = 8               # ...this many at a time
FIND_PER_HOST = 4              # so one big site can't eat the whole budget
FIND_MAX_ITEMS = 800
FIND_MAX_CARDS_PER_PAGE = 120
FIND_CACHE_DIR = os.path.join(MS_CACHE_HOME, "find")
FIND_CACHE_TTL = 6 * 3600      # mined pages
FIND_SEARCH_TTL = 3 * 3600     # the search engine's own answer
FIND_MIN_THUMB = 90            # px: an <img> smaller than this is furniture

# The search engines, in the order they are tried. All three answer a plain
# GET without an API key, and each one is asked with its own safe-search-off
# switch — without that a search for a performer comes back empty. They are a
# chain rather than a choice because the free endpoints rate-limit hard: the
# first one to actually return results wins, and one that starts refusing is
# skipped for a while (see `_search_cooldown`).
_SEARCH_ENGINES = (
    ("duckduckgo", "https://html.duckduckgo.com/html/?q={q}&kp=-2&kl=us-en"),
    ("brave", "https://search.brave.com/search?q={q}&safesearch=off"),
    ("searxng", "https://searxng.site/search?q={q}&safesearch=0"),
)
SEARCH_COOLDOWN = 600          # an engine that refuses is left alone this long
_search_cooldown = {}          # engine name -> time it may be asked again
_search_cooldown_lock = threading.Lock()

# Hosts that are never a thumbnail grid: other search engines, the encyclopedia
# entry, and the social sites whose grids are built by JavaScript we can't run.
_FIND_SKIP_HOSTS = (
    "duckduckgo.com", "google.", "bing.com", "yandex.", "baidu.com",
    "yahoo.com", "ecosia.org", "startpage.com", "brave.com",
    "wikipedia.org", "wikidata.org", "imdb.com", "fandom.com",
    "twitter.com", "x.com", "facebook.com", "instagram.com", "tiktok.com",
    "pinterest.", "reddit.com", "t.me", "onlyfans.com", "linktr.ee",
    "youtube.com", "youtu.be", "dailymotion.com", "vimeo.com",
    "amazon.", "ebay.", "aliexpress.",
)

_IMG_TAG_RE = re.compile(r"<img\b[^>]*>", re.I)
_META_TAG_RE = re.compile(r"<meta\b[^>]*>", re.I)
_DURATION_RE = re.compile(r"\b(\d{1,3}:[0-5]\d(?::[0-5]\d)?)\b")
_DDG_WRAP_RE = re.compile(r"/l/\?(?:.*&)?uddg=([^&\"']+)", re.I)

# What the path says the link is. Checked against the URL and the thumbnail.
_PHOTO_HINT_RE = re.compile(
    r"/(gallery|galleries|photo|photos|pic|pics|picture|pictures|album|albums"
    r"|image|images|gal|set|sets|shoot|shoots|foto|fotos)(?:[/_-]|\d|$)", re.I)
_VIDEO_HINT_RE = re.compile(
    r"/(video|videos|watch|movie|movies|scene|scenes|clip|clips|embed|play"
    r"|player|media|v|vid)(?:[/_-]|\d|$)", re.I)
# Images that are part of the furniture rather than of the content.
_JUNK_IMG_RE = re.compile(
    r"(logo|sprite|avatar|icon|favicon|banner|placeholder|blank|spacer|loading"
    r"|pixel|1x1|transparent|/ads?/|adserv|smilie|emoji|flag)", re.I)

# Image attributes in the order a lazy-loading grid fills them: the real URL
# hides in a data- attribute while `src` holds a grey placeholder.
_IMG_SRC_ATTRS = ("data-original", "data-src", "data-lazy-src", "data-lazy",
                  "data-thumb", "data-thumb-url", "data-thumb_url",
                  "data-thumbnail", "data-image", "data-poster", "data-url",
                  "src")

# Those same attributes also carry the little clip that plays on hover. It is
# not something an <img> can show, so a candidate ending in one of these is
# skipped and the next attribute gets its turn.
_NOT_AN_IMAGE_RE = re.compile(r"\.(mp4|webm|m3u8|ts|mov|mkv)(?:$|[?#])", re.I)

# Script and style hold anchors and text that are not on the page at all —
# thumbnail templates, tracking snippets — and mining them yields cards whose
# title is a line of JavaScript.
_SCRIPT_STYLE_RE = re.compile(r"<(script|style|noscript)\b[^>]*>.*?</\1>",
                              re.I | re.S)

_find_jobs = {}
_find_jobs_lock = threading.Lock()


def _attr(tag, name):
    """One attribute out of a raw tag, quoted or not."""
    import html as _html
    m = re.search(r"""\b%s\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))"""
                  % re.escape(name), tag, re.I)
    if not m:
        return ""
    return _html.unescape(m.group(1) or m.group(2) or m.group(3) or "").strip()


def _largest_in_srcset(value):
    """The biggest candidate of a srcset: the one with the widest descriptor,
    and failing that the last, which is the convention."""
    best, best_w = "", -1.0
    for part in (value or "").split(","):
        bits = part.strip().split()
        if not bits:
            continue
        try:
            w = float(bits[1].rstrip("wx")) if len(bits) > 1 else 0.0
        except ValueError:
            w = 0.0
        if w >= best_w:
            best, best_w = bits[0], w
    return best


def _img_thumb(tag, page_url):
    """The picture an <img> really shows, absolute — or "" when it is
    furniture, a placeholder, or too small to be a card."""
    raw = ""
    for name in _IMG_SRC_ATTRS:
        v = _attr(tag, name)
        if (v and len(v) > 8 and not v.lower().startswith("data:")
                and not _NOT_AN_IMAGE_RE.search(v)):
            raw = v
            break
    if not raw:
        raw = _largest_in_srcset(_attr(tag, "srcset") or _attr(tag, "data-srcset"))
    if not raw or raw.lower().startswith(("data:", "javascript:")):
        return ""
    for dim in ("width", "height"):
        try:
            n = int(re.sub(r"[^0-9]", "", _attr(tag, dim)) or 0)
        except ValueError:
            n = 0
        if 0 < n < FIND_MIN_THUMB:
            return ""
    url = urllib.parse.urljoin(page_url, raw).split("#")[0]
    if not url.lower().startswith(("http://", "https://")):
        return ""
    if _JUNK_IMG_RE.search(urllib.parse.urlsplit(url).path):
        return ""
    return url


def _site_of(url):
    """The site a URL belongs to: the last two labels of its host, so a link
    from cdn.example.com to www.example.com still counts as staying home."""
    try:
        host = urllib.parse.urlsplit(url).netloc.lower().split(":")[0]
    except ValueError:
        return ""
    parts = [p for p in host.split(".") if p]
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def _is_index_link(url):
    """True for a link to a category, tag or profile page rather than to a
    thing you can watch. Those come with a picture of their own and would
    otherwise sit in the grid as a card titled "Anal" or "teens".

    The test is deliberately narrow: a short path whose marker sits at the
    front (/category/anal, /pornstars/riley-reid). A long one that merely
    passes through the same word (/models/riley/video/123) is a video."""
    kind, _name = _is_taxonomy_path(url)
    if not kind:
        return False
    try:
        segs = [x for x in urllib.parse.urlsplit(url).path.split("/") if x]
    except ValueError:
        return False
    return len(segs) <= 3 and not segs[-1].isdigit()


def _card_kind(href, thumb, page_kind):
    """video or photo. The path says it when it can, and anything still
    unnamed inherits whatever the page it came from is."""
    try:
        path = urllib.parse.urlsplit(href).path
    except ValueError:
        return page_kind
    if _PHOTO_HINT_RE.search(path):
        return "photo"
    if _VIDEO_HINT_RE.search(path):
        return "video"
    if thumb and _PHOTO_HINT_RE.search(urllib.parse.urlsplit(thumb).path):
        return "photo"
    return page_kind


def _proxied_thumb(url, referer):
    """Thumbnails are hotlink-protected on most of these sites: requested
    straight from the page they 403, requested through the proxy with the page
    they came from as Referer they load."""
    return "/api/proxy?p=" + encode_target(url, {
        "User-Agent": DEFAULT_UA,
        "Referer": referer,
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    })


def _og(text, prop):
    """One OpenGraph value out of a page's head."""
    for tag in _META_TAG_RE.findall(text[:200000]):
        if (_attr(tag, "property") or _attr(tag, "name")).lower() == prop:
            return _attr(tag, "content")
    return ""


_CODE_ISH_RE = re.compile(r"(document\.|function\s*\(|\{|\}|=>|;\s*$|</)")


def _best_title(label, url):
    """The label a card should carry.

    Markup often hands over a badge ("1080p", "HD") or a line of a template
    instead of a title, while the URL's own slug is the title on most of these
    sites. So the label has to earn its place: real words, several of them,
    nothing that looks like code."""
    label = (label or "").strip()
    slug = _pretty_slug(url)
    if label and not _CODE_ISH_RE.search(label):
        if len(_WORD_RE.findall(label)) >= 3 or len(label) >= len(slug):
            return label
    return slug or label


def _find_cards(page_url, text):
    """Every card a page shows: {url, title, thumb, kind, dur}.

    A card is an <a> with an <img> inside it, pointing somewhere on the same
    site. That one rule covers a tube's video grid, a performer's profile page
    and a gallery index without knowing which of the three it is looking at —
    and it leaves out navigation, ads and the sidebar, none of which are a
    picture linking deeper into the site."""
    import html as _html
    text = _SCRIPT_STYLE_RE.sub(" ", text)
    site = _site_of(page_url)
    page_kind = "photo" if _PHOTO_HINT_RE.search(
        urllib.parse.urlsplit(page_url).path) else "video"
    out, seen = [], set()

    # The page itself is a card when it is one video: a watch page the search
    # engine returned carries its poster and title in the OpenGraph tags, and
    # nothing in its own grid points back at it.
    og_img = "" if _is_index_link(page_url) else _og(text, "og:image")
    if og_img:
        own = urllib.parse.urljoin(page_url, og_img)
        if own.lower().startswith(("http://", "https://")):
            kind = "video" if (_og(text, "og:type") or "").lower().startswith(
                "video") else page_kind
            out.append({"url": page_url, "thumb": own, "kind": kind, "dur": "",
                        "title": _og(text, "og:title") or _pretty_slug(page_url)})
            seen.add(_norm_url(page_url))

    for m in _A_TAG_RE.finditer(text):
        if len(out) >= FIND_MAX_CARDS_PER_PAGE:
            break
        attrs, inner = m.group(1), m.group(2)
        hm = _HREF_RE.search(attrs)
        if not hm:
            continue
        href = (hm.group(1) or hm.group(2) or "").strip()
        if not href or href[0] in "#?" or href.lower().startswith(
                ("javascript:", "mailto:", "data:")):
            continue
        url = urllib.parse.urljoin(page_url, _html.unescape(href)).split("#")[0]
        if not url.lower().startswith(("http://", "https://")):
            continue
        if _NON_PAGE_RE.search(url) or _site_of(url) != site:
            continue                    # off-site here means an ad, not a card
        if _is_index_link(url):
            continue                    # a category tile is not a card
        key = _norm_url(url)
        if key in seen:
            continue
        img = _IMG_TAG_RE.search(inner)
        if not img:
            continue
        thumb = _img_thumb(img.group(0), page_url)
        if not thumb:
            continue
        inner_text = re.sub(r"\s+", " ", _html.unescape(
            _TAG_STRIP_RE.sub(" ", inner))).strip()
        title = _best_title(
            (_attr(img.group(0), "alt") or _attr(img.group(0), "title")
             or _attr(attrs, "title") or _attr(attrs, "aria-label")
             or inner_text)[:200], url)
        dm = _DURATION_RE.search(inner_text)
        seen.add(key)
        out.append({"url": url, "title": title, "thumb": thumb,
                    "kind": _card_kind(url, thumb, page_kind),
                    "dur": dm.group(1) if dm else ""})
    return out


def _find_cache_path(kind, key):
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return os.path.join(FIND_CACHE_DIR, "%s-%s.json" % (kind, digest))


def _find_cache_get(kind, key, ttl):
    path = _find_cache_path(kind, key)
    try:
        if time.time() - os.path.getmtime(path) > ttl:
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _find_cache_put(kind, key, data):
    try:
        os.makedirs(FIND_CACHE_DIR, exist_ok=True)
        with open(_find_cache_path(kind, key), "w", encoding="utf-8") as f:
            json.dump(data, f)
    except (OSError, ValueError, TypeError):
        pass


# The engine's "we think you are a robot" page: HTTP 202, no results, and a
# form that wants JavaScript run. Worth recognising, because it arrives looking
# like a perfectly good response.
_SEARCH_BLOCKED = "anomaly.js"


def _search_fetch(url):
    """One search request, as a plain GET.

    Impersonation goes FIRST here, which is the opposite of the rule everywhere
    else in this file: the plain client is recognised by its TLS handshake and
    handed the robot page every time, while an impersonated Chrome gets the
    real result list. And it is asked with no headers of ours at all — a
    hand-written User-Agent next to a Chrome handshake is itself the tell. The
    plain client still gets its turn afterwards, so a machine without curl_cffi
    is not left with nothing."""
    max_bytes = 2 * 1024 * 1024

    def usable(text):
        return text if text and _SEARCH_BLOCKED not in text else ""

    if _IMPERSONATE is not None:
        try:
            from curl_cffi import requests as curl_requests
            r = curl_requests.get(url, timeout=15, impersonate="chrome")
            if r.status_code < 400:
                got = usable(r.content[:max_bytes].decode("utf-8", "replace"))
                if got:
                    return got
        except Exception:  # noqa: BLE001
            pass
    headers = {
        "User-Agent": DEFAULT_UA,
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
    }
    if _HAS_REQUESTS:
        sess = _acquire_session()
        try:
            r = sess.get(url, headers=headers, timeout=(5, 12))
            if r.status_code < 400:
                got = usable(r.content[:max_bytes].decode("utf-8", "replace"))
                if got:
                    return got
        except Exception:  # noqa: BLE001
            pass
        finally:
            _release_session(sess)
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=12) as resp:
            return usable(resp.read(max_bytes).decode("utf-8", "replace"))
    except Exception:  # noqa: BLE001
        return ""


def _engine_links(text):
    """The outside pages a results page points at.

    Deliberately not per-engine markup: every absolute link that is not the
    engine's own furniture is a candidate, and the junk that gets through is
    dropped later by the same rules that drop it everywhere else. One engine
    wraps its results in a redirector (/l/?uddg=<encoded>), so that one form
    has to be unwrapped to find the real address."""
    import html as _html
    links, seen = [], set()
    for m in _HREF_RE.finditer(text):
        href = _html.unescape((m.group(1) or m.group(2) or "").strip())
        wrap = _DDG_WRAP_RE.search(href)
        if wrap:
            href = urllib.parse.unquote(wrap.group(1))
        if href.startswith("//"):
            href = "https:" + href
        if not href.lower().startswith(("http://", "https://")):
            continue
        host = urllib.parse.urlsplit(href).netloc.lower()
        if any(s in host for s in _FIND_SKIP_HOSTS) or _NON_PAGE_RE.search(href):
            continue
        key = _norm_url(href)
        if key in seen:
            continue
        seen.add(key)
        links.append(href)
    return links


def _search_links(query, fresh=False):
    """Page URLs the web offers for `query` — the first engine that answers.

    Three results would be an engine that answered with its own navigation and
    nothing else, so that counts as a refusal and the next one is tried."""
    if not fresh:
        cached = _find_cache_get("q", query, FIND_SEARCH_TTL)
        if cached is not None:
            return cached

    now = time.time()
    for name, template in _SEARCH_ENGINES:
        with _search_cooldown_lock:
            if _search_cooldown.get(name, 0) > now:
                continue
        links = _engine_links(_search_fetch(
            template.format(q=urllib.parse.quote(query))))
        if len(links) > 3:
            _find_cache_put("q", query, links)
            return links
        with _search_cooldown_lock:
            _search_cooldown[name] = time.time() + SEARCH_COOLDOWN
        sys.stderr.write("  FIND %s refused the search - trying the next engine\n"
                         % name)
    return []


def _find_rank_pages(links, words):
    """Profile and tag pages first: a page filed under the name is a grid of
    nothing but that person, while a watch page is one card plus whatever the
    site felt like recommending next to it."""
    def rank(url):
        low = urllib.parse.unquote(url).lower()
        kind, _name = _is_taxonomy_path(url)
        score = 6 if kind else 0
        hits = sum(1 for w in words if w in low)
        score += 3 * hits
        if words and hits == len(words):
            score += 4
        if len([x for x in urllib.parse.urlsplit(url).path.split("/") if x]) <= 1:
            score -= 2              # a bare home page rarely carries the grid
        return -score
    return sorted(links, key=rank)


def _find_page_cards(page_url, fresh=False):
    """The cards of one page, from disk while they are still fresh."""
    if not fresh:
        cached = _find_cache_get("p", _norm_url(page_url), FIND_CACHE_TTL)
        if cached is not None:
            return cached
    try:
        text, _imp = _fetch_page_html(page_url)
    except Exception:  # noqa: BLE001
        _find_cache_put("p", _norm_url(page_url), [])  # don't retry it all day
        return []
    cards = _find_cards(page_url, text)
    _find_cache_put("p", _norm_url(page_url), cards)
    return cards


def _set_find(job_id, **fields):
    with _find_jobs_lock:
        job = _find_jobs.get(job_id)
        if job:
            job.update(fields)


def _find_collect(job_id, page_url, cards, words, seen_card):
    """Turn one page's cards into results the browser can paint, and hand them
    to the job as soon as they exist — that is what makes the grid fill in
    while the rest of the pages are still being fetched."""
    ready = []
    for c in cards:
        key = _norm_url(c["url"])
        if key in seen_card or len(seen_card) >= FIND_MAX_ITEMS:
            continue
        seen_card.add(key)
        low = (c["title"] + " " + urllib.parse.unquote(c["url"])).lower()
        hits = sum(1 for w in words if w in low)
        ready.append({
            "url": c["url"],
            "title": c["title"],
            "thumb": _proxied_thumb(c["thumb"], page_url),
            "kind": c["kind"],
            "dur": c.get("dur") or "",
            "site": urllib.parse.urlsplit(c["url"]).netloc.lower(),
            "via": page_url,
            "score": hits + (2 if words and hits == len(words) else 0),
        })
    # Within one page the cards that actually name the search go first, so the
    # profile grid leads and whatever the site had in its sidebar follows.
    ready.sort(key=lambda c: -c["score"])
    with _find_jobs_lock:
        job = _find_jobs.get(job_id)
        if job:
            job["items"].extend(ready)
            job["done"] += 1
            job["found"] = len(job["items"])


def _run_find_job(job_id, name, fresh):
    """Search the web for the name, then mine every page it returned."""
    words = {w for w in _WORD_RE.findall(name.lower()) if len(w) > 1}
    try:
        _set_find(job_id, stage="searching")
        links, seen_link = [], set()
        for query in (name, '"%s" videos' % name, '"%s" photos gallery' % name):
            for url in _search_links(query, fresh):
                key = _norm_url(url)
                if key not in seen_link:
                    seen_link.add(key)
                    links.append(url)
        if not links:
            return _set_find(job_id, stage="error", error=(
                "the web search came back empty — it is probably rate-limiting "
                "this machine. Give it a minute and try again."))

        pages, per_host = [], {}
        for url in _find_rank_pages(links, words):
            site = _site_of(url)
            if per_host.get(site, 0) >= FIND_PER_HOST:
                continue
            per_host[site] = per_host.get(site, 0) + 1
            pages.append(url)
            if len(pages) >= FIND_PAGES:
                break

        _set_find(job_id, stage="mining", total=len(pages))
        seen_card = set()
        with ThreadPoolExecutor(max_workers=FIND_WORKERS) as ex:
            futures = {ex.submit(_find_page_cards, u, fresh): u for u in pages}
            for fut in as_completed(futures):
                page_url = futures[fut]
                try:
                    _find_collect(job_id, page_url, fut.result(), words, seen_card)
                except Exception:  # noqa: BLE001
                    with _find_jobs_lock:
                        job = _find_jobs.get(job_id)
                        if job:
                            job["done"] += 1
        _set_find(job_id, stage="done")
    except Exception as e:  # noqa: BLE001
        _set_find(job_id, stage="error", error=str(e).splitlines()[-1][:250])


# ---------- Cast: mirror the wall to the TV, picture only ----------
#
# The whole screen goes to the TV as ONE H.264 stream and the sound stays on
# the PC — not by routing audio anywhere, but because the stream has no audio
# track at all (-an). Nothing is installed on the TV: it already exposes a
# DLNA MediaRenderer, so we hand it an HLS URL over UPnP AVTransport and it
# plays. Verified on the UN43NU7100: a file with no audio track plays, a live
# never-ending playlist plays, and GetTransportInfo says PLAYING while it does.
#
# Two locks keep this from ever landing on the wrong screen (there is a second
# Samsung on this LAN, and it also answers as a MediaRenderer):
#   1. the target is pinned by UUID *and* MAC, checked again before every push.
#      A Samsung shows a different UUID per service and its name is editable
#      from the remote, so neither is enough alone.
#   2. the HLS files come from their own listener, bound to the LAN interface
#      (not 0.0.0.0), that answers the paired TV's IP and nobody else. The main
#      server, with the proxy and local files, stays on 127.0.0.1.
#
# Nothing here is cast "automatically": no pairing without a click, no
# fallback to "the first TV that answered", no guessing between two matches.

from xml.etree import ElementTree as _ET

CAST_DEVICE_FILE = os.path.join(MS_CACHE_HOME, "cast_device.json")
CAST_PID_FILE = os.path.join(MS_CACHE_HOME, "cast_ffmpeg.pid")
CAST_PORT_OFFSET = 50          # cast listener = main port + this (then +1…+9)
CAST_FPS = 30
CAST_HEIGHT = 1080             # the TV upscales; 4K would quadruple the encode
CAST_MAXRATE = "10M"
CAST_SEGMENT_SECONDS = 1
CAST_LIST_SIZE = 4
CAST_WARMUP_SEGMENTS = 3       # the Samsung wants a few segments before it starts
CAST_START_TIMEOUT = 25        # seconds for ffmpeg to produce those
CAST_FIRST_FETCH_GRACE = 12    # accepted but never fetched by then = firewall
CAST_POLL_SECONDS = 3
CAST_PLAY_BUDGET = 12          # seconds to get from SetAVTransportURI to PLAYING
CAST_RECONNECT_EVERY = 10      # while the TV is away: knock this often…
CAST_RECONNECT_BUDGET = 180    # …for this long, then give up
CAST_UNREACHABLE_POLLS = 2     # consecutive failed GetTransportInfo = TV is away
# The owner's mark: a word the TV's name MUST carry, on top of UUID and MAC.
# A TV without it cannot even be paired. Comma-separated, case-insensitive.
CAST_OWNER_MARKS = tuple(m.strip().lower() for m in
                         os.environ.get("MULTISCREEN_CAST_OWNER", "dani").split(",") if m.strip())
# A screen cast needs a test pattern that reached THIS TV within this many
# minutes (and since this server started). 0 turns the requirement off.
CAST_REQUIRE_TEST_MINUTES = int(os.environ.get("MULTISCREEN_CAST_REQUIRE_TEST_MINUTES", "30"))
CAST_AUDIT_FILE = os.path.join(MS_CACHE_HOME, "cast_audit.log")
CAST_MAC_CACHE_SECONDS = 30
CAST_MAX_RESTARTS = 3
CAST_REPUSH_COOLDOWN = 30
SSDP_ADDR, SSDP_PORT = "239.255.255.250", 1900
UPNP_DEV_NS = "urn:schemas-upnp-org:device-1-0"
AVT_SERVICE = "urn:schemas-upnp-org:service:AVTransport:1"
# What the renderer is told about the stream, and what our HTTP answers carry.
# OP=01 (byte-range seek) in the headers is what the NU7100 was tested with.
DLNA_FEATURES = "DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000"

_cast_lock = threading.Lock()
_cast = {
    "state": "idle",           # idle | starting | casting | reconnecting | error
    "source": "screen",        # screen | test (the colour-bar pattern)
    "device": None,            # the verified target of this session
    "anchor": None,            # (segment no., mtime) of the first .ts the TV took
    "latency": None,           # seconds between capture and the TV showing it
    "reconnect_since": None,
    "checks": [],              # the locks the last verification passed, in order
    "url": None, "dir": None, "proc": None, "log": None,
    "capture": None, "encoder": None, "fps": None, "speed": None,
    "tv_state": None, "tv_pos": None,
    "served": 0, "first_fetch": None, "started": None,
    "restarts": 0, "repushes": 0,
    "warning": None, "error": None,
}
_cast_server = None            # (httpd, ip, port) of the LAN listener
_cast_last_test = None         # {"uuid", "mac", "at"}: the last test pattern the TV played
_cast_mac_cache = {}           # ip -> (mac, looked_up_at), for the listener's MAC check
_cast_capture_mode = None      # "ddagrab" | "gdigrab" | "x11grab", probed once


class CastRefused(Exception):
    """The lock (or a precondition) said no. The message is shown verbatim."""


class CastAbsent(CastRefused):
    """Nothing wrong with the identity — the paired TV just isn't answering.
    The only refusal the monitor is allowed to wait out."""


# --- audit trail ---------------------------------------------------------------

def _cast_audit(event, device=None, **fields):
    """One line per push, refusal and stop: who, when, where to. Never raises."""
    try:
        os.makedirs(MS_CACHE_HOME, exist_ok=True)
        parts = [time.strftime("%Y-%m-%dT%H:%M:%S"), event]
        if device:
            parts.append("%s uuid=%s mac=%s ip=%s" % (device.get("name"), device.get("uuid"),
                                                     device.get("mac"), device.get("ip")))
        parts += ["%s=%s" % (k, v) for k, v in fields.items() if v is not None]
        with open(CAST_AUDIT_FILE, "a", encoding="utf-8") as f:
            f.write("  ".join(str(p) for p in parts) + "\n")
    except Exception:  # noqa: BLE001
        pass


# --- identity -------------------------------------------------------------


def _cast_load_device():
    try:
        with open(CAST_DEVICE_FILE, encoding="utf-8") as f:
            rec = json.load(f)
        if rec.get("uuid") and rec.get("mac"):
            return rec
    except (OSError, ValueError):
        pass
    return None


def _cast_save_device(rec):
    os.makedirs(MS_CACHE_HOME, exist_ok=True)
    with open(CAST_DEVICE_FILE, "w", encoding="utf-8") as f:
        json.dump(rec, f, indent=2)


def _cast_clear_device():
    try:
        os.remove(CAST_DEVICE_FILE)
    except OSError:
        pass


def _cast_mac_for(ip):
    """MAC of `ip` from the ARP table — we just fetched its description, so
    the entry exists. Normalised to AA-BB-CC-DD-EE-FF."""
    try:
        if os.name == "nt":
            out = subprocess.run(["arp", "-a", ip], capture_output=True,
                                 text=True, timeout=5).stdout
        else:
            out = subprocess.run(["ip", "neigh", "show", ip], capture_output=True,
                                 text=True, timeout=5).stdout
    except Exception:  # noqa: BLE001
        return None
    m = re.search(r"((?:[0-9A-Fa-f]{2}[-:]){5}[0-9A-Fa-f]{2})", out)
    return m.group(1).upper().replace(":", "-") if m else None


def _cast_describe(location):
    """One renderer from its UPnP description URL, or None if it has no
    AVTransport (then it cannot be told to play anything)."""
    try:
        with urllib.request.urlopen(location, timeout=5) as r:
            root = _ET.fromstring(r.read())
    except Exception:  # noqa: BLE001
        return None
    ns = {"u": UPNP_DEV_NS}
    dev = root.find("u:device", ns)
    if dev is None:
        return None
    control = None
    for svc in dev.iter("{%s}service" % UPNP_DEV_NS):
        stype = svc.findtext("u:serviceType", "", ns) or ""
        if stype.startswith("urn:schemas-upnp-org:service:AVTransport"):
            control = urllib.parse.urljoin(location, svc.findtext("u:controlURL", "", ns) or "")
    if not control:
        return None
    ip = urllib.parse.urlparse(location).hostname
    return {
        "uuid": (dev.findtext("u:UDN", "", ns) or "").strip(),
        "name": (dev.findtext("u:friendlyName", "", ns) or "").strip(),
        "model": (dev.findtext("u:modelName", "", ns) or "").strip(),
        "ip": ip,
        "mac": _cast_mac_for(ip),
        "control": control,
    }


def cast_discover(timeout=3.0):
    """Every DLNA MediaRenderer on the LAN right now."""
    msg = ("M-SEARCH * HTTP/1.1\r\n"
           "HOST: %s:%d\r\n"
           "MAN: \"ssdp:discover\"\r\n"
           "MX: 2\r\n"
           "ST: urn:schemas-upnp-org:device:MediaRenderer:1\r\n\r\n"
           % (SSDP_ADDR, SSDP_PORT)).encode("ascii")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    locations = []
    try:
        for _ in range(2):
            sock.sendto(msg, (SSDP_ADDR, SSDP_PORT))
        while True:
            try:
                data, _addr = sock.recvfrom(65507)
            except socket.timeout:
                break
            for line in data.decode("utf-8", "ignore").split("\r\n"):
                if line.lower().startswith("location:"):
                    loc = line.split(":", 1)[1].strip()
                    if loc and loc not in locations:
                        locations.append(loc)
    finally:
        sock.close()
    found = []
    for loc in locations:
        dev = _cast_describe(loc)
        if dev and dev not in found:
            found.append(dev)
    return found


def _cast_owner_mark(name):
    """The owner mark found in `name`, or None."""
    low = (name or "").lower()
    for mark in CAST_OWNER_MARKS:
        if mark in low:
            return mark
    return None


def _cast_check(paired, dev):
    """Why `dev` may NOT stand in for the paired TV — None when it may.
    Each line is an independent lock; the first that fails is the answer."""
    if dev["uuid"] != paired["uuid"]:
        return "UUID differs"
    if not dev.get("mac"):
        return "its MAC could not be read from the ARP table"
    if dev["mac"] != paired["mac"]:
        return ("UUID matches but the MAC changed (%s now, %s when paired)"
                % (dev["mac"], paired["mac"]))
    if dev["name"] != paired["name"]:
        return ("UUID matches but the name changed (%r now, %r when paired)"
                % (dev["name"], paired["name"]))
    if CAST_OWNER_MARKS and not _cast_owner_mark(dev["name"]):
        return "the name %r carries none of the owner marks %s" % (dev["name"], list(CAST_OWNER_MARKS))
    if paired.get("model") and dev.get("model") != paired["model"]:
        return ("UUID matches but the model changed (%r now, %r when paired)"
                % (dev.get("model"), paired["model"]))
    host = urllib.parse.urlparse(dev["control"]).hostname
    if host != dev["ip"]:
        return "its control URL points at %s, not at the device itself (%s)" % (host, dev["ip"])
    return None


def cast_verify_target():
    """The one renderer allowed to receive the stream, verified on the network
    this very moment. Refuses rather than guess, and records which locks the
    winner passed so the UI can show them."""
    paired = _cast_load_device()
    if not paired:
        raise CastRefused("no TV paired yet — open Cast and pair yours first.")
    devices = cast_discover()
    if not devices:
        raise CastAbsent("no DLNA renderer answered on the network. Is the TV on?")
    ok = [d for d in devices if _cast_check(paired, d) is None]
    if len(ok) > 1:
        _cast_audit("refused", None, reason="more than one device matches")
        raise CastRefused("more than one device matches the paired TV — refusing to guess.")
    if not ok:
        same = [d for d in devices if d["uuid"] == paired["uuid"]]
        if same:
            why = _cast_check(paired, same[0])
            _cast_audit("refused", same[0], reason=why)
            raise CastRefused("refused: " + why)
        raise CastAbsent("the paired TV (%s) is not on the network — %d other renderer(s) "
                         "answered and were ignored." % (paired["name"], len(devices)))
    dev = ok[0]
    checks = [
        "UUID matches (…%s)" % dev["uuid"][-12:],
        "MAC matches (%s)" % dev["mac"],
        "name matches (%s)" % dev["name"],
        "model matches (%s)" % dev.get("model"),
        "only one match among %d renderer(s)" % len(devices),
        "control URL is on the device itself (%s)" % dev["ip"],
    ]
    mark = _cast_owner_mark(dev["name"])
    if mark:
        checks.insert(3, "owner mark '%s' in the name" % mark)
    with _cast_lock:
        _cast["checks"] = checks
    return dev


def cast_pair(uuid_):
    """Pin one renderer as THE target. Only a device answering right now can
    be paired (the MAC comes from talking to it), and only one whose name
    carries the owner's mark — so the wrong TV cannot be paired by a slip."""
    if not uuid_:
        raise CastRefused("no device chosen.")
    match = [d for d in cast_discover() if d["uuid"] == uuid_]
    if not match:
        raise CastRefused("that device did not answer now — only a TV that is on "
                          "and reachable can be paired.")
    if len(match) > 1:
        raise CastRefused("two devices claim the same UUID — refusing to pair.")
    dev = match[0]
    if not dev["mac"]:
        raise CastRefused("could not read the TV's MAC address from the ARP table; "
                          "pairing needs it.")
    if CAST_OWNER_MARKS and not _cast_owner_mark(dev["name"]):
        _cast_audit("pair-refused", dev, reason="no owner mark")
        raise CastRefused("refused to pair %r: its name carries none of the owner marks %s. "
                          "This lock exists so a TV that is not yours can never be paired "
                          "by a slip of the mouse." % (dev["name"], list(CAST_OWNER_MARKS)))
    rec = {"uuid": dev["uuid"], "mac": dev["mac"], "name": dev["name"],
           "model": dev["model"], "ip": dev["ip"], "paired_at": time.time()}
    _cast_save_device(rec)
    _cast_audit("paired", dev)
    return rec


def _cast_soap(control, action, body=""):
    envelope = (
        '<?xml version="1.0"?>'
        '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
        's:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body>'
        '<u:%s xmlns:u="%s"><InstanceID>0</InstanceID>%s</u:%s>'
        "</s:Body></s:Envelope>" % (action, AVT_SERVICE, body, action))
    req = urllib.request.Request(control, data=envelope.encode("utf-8"), headers={
        "Content-Type": 'text/xml; charset="utf-8"',
        "SOAPACTION": '"%s#%s"' % (AVT_SERVICE, action),
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.read().decode("utf-8", "ignore")
    except urllib.error.HTTPError as exc:
        # A UPnP fault is an HTTP 500 whose body carries the real reason.
        body = ""
        try:
            body = exc.read().decode("utf-8", "ignore")
        except Exception:  # noqa: BLE001
            pass
        code = re.search(r"<errorCode>([^<]*)<", body)
        desc = re.search(r"<errorDescription>([^<]*)<", body)
        raise CastRefused("the TV refused %s: %s%s" % (
            action,
            desc.group(1).strip() if desc else "HTTP %d" % exc.code,
            " (UPnP error %s)" % code.group(1).strip() if code else ""))


def _cast_didl(url, title):
    """DIDL-Lite metadata for a live HLS stream: a broadcast, no byte seek."""
    item = (
        '<DIDL-Lite xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/">'
        '<item id="0" parentID="-1" restricted="1">'
        "<dc:title>%s</dc:title>"
        "<upnp:class>object.item.videoItem.videoBroadcast</upnp:class>"
        '<res protocolInfo="http-get:*:application/vnd.apple.mpegurl:DLNA.ORG_OP=00;'
        'DLNA.ORG_FLAGS=01700000000000000000000000000000">%s</res>'
        "</item></DIDL-Lite>" % (title, url))
    return item.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _cast_push(device, url):
    """Hand the URL to the TV and make sure it plays.

    After SetAVTransportURI the NU7100 goes TRANSITIONING and usually starts
    on its own; a Play sent during that moment is answered with UPnP 701
    "Transition not available" (seen on the real TV). So: wait the transition
    out, skip Play if the TV already plays, otherwise Play — and give a 701 a
    few more tries before giving up."""
    ctrl = device["control"]
    _cast_soap(ctrl, "SetAVTransportURI",
               "<CurrentURI>%s</CurrentURI><CurrentURIMetaData>%s</CurrentURIMetaData>"
               % (url, _cast_didl(url, "MultiScreen")))
    # The TV must now report OUR url as its current media. Anything else —
    # or a different device having answered — and we stop right here.
    try:
        info = _cast_soap(ctrl, "GetMediaInfo")
    except Exception:  # noqa: BLE001
        info = ""
    echo = re.search(r"<CurrentURI>([^<]*)<", info)
    echo = echo.group(1).strip().replace("&amp;", "&") if echo else ""
    if echo and echo != url:
        try:
            _cast_soap(ctrl, "Stop")
        except Exception:  # noqa: BLE001
            pass
        _cast_audit("refused", device, reason="URI echo mismatch", got=echo)
        raise CastRefused("the TV reports a different media URI than the one handed to it "
                          "(%s) — stopping." % echo)
    with _cast_lock:
        _cast["checks"] = [c for c in _cast["checks"] if not c.startswith("TV echoes")]
        _cast["checks"].append("TV echoes our URL back" if echo else "TV echoes: not reported")
    _cast_audit("push", device, source=_cast["source"], url=url)
    deadline = time.time() + CAST_PLAY_BUDGET
    last = None
    while time.time() < deadline:
        try:
            state, _pos = _cast_tv_state(ctrl)
        except Exception:  # noqa: BLE001
            state = None
        if state == "PLAYING":
            return
        if state == "TRANSITIONING":
            time.sleep(0.5)
            continue
        try:
            _cast_soap(ctrl, "Play", "<Speed>1</Speed>")
            return
        except CastRefused as exc:
            last = exc
            time.sleep(1.0)
    raise last or CastRefused("the TV did not start playing in time.")


def _cast_tv_state(control):
    """(transport state, position) as the TV itself reports them."""
    info = _cast_soap(control, "GetTransportInfo")
    st = re.search(r"<CurrentTransportState>([^<]*)<", info)
    pos = _cast_soap(control, "GetPositionInfo")
    rel = re.search(r"<RelTime>([^<]*)<", pos)
    return (st.group(1) if st else None, rel.group(1) if rel else None)


# --- the screen encoder -----------------------------------------------------


def _cast_input_args(mode):
    if mode == "ddagrab":
        # Desktop Duplication: the frame is grabbed on the GPU. It comes out as
        # a D3D11 surface, so it is downloaded before the encoder; the scale
        # and pixel-format steps ride along in the same graph.
        return ["-init_hw_device", "d3d11va=dx", "-filter_complex",
                "ddagrab=output_idx=0:framerate=%d,hwdownload,format=bgra,%s"
                % (CAST_FPS, _cast_vf())]
    if mode == "gdigrab":
        return ["-f", "gdigrab", "-framerate", str(CAST_FPS), "-i", "desktop",
                "-vf", _cast_vf()]
    return ["-f", "x11grab", "-framerate", str(CAST_FPS),
            "-i", os.environ.get("DISPLAY", ":0"), "-vf", _cast_vf()]


def _cast_vf():
    return "scale=-2:%d:flags=fast_bilinear,format=nv12" % CAST_HEIGHT


def _cast_probe_capture():
    """Which screen grabber this ffmpeg/OS can actually run, found once by
    grabbing a single frame. GPU first, GDI as the fallback."""
    global _cast_capture_mode
    if _cast_capture_mode:
        return _cast_capture_mode
    modes = ["ddagrab", "gdigrab"] if os.name == "nt" else ["x11grab"]
    for mode in modes:
        cmd = ([FFMPEG, "-hide_banner", "-loglevel", "error"] + _cast_input_args(mode)
               + ["-frames:v", "1", "-f", "null", "-"])
        try:
            rc = subprocess.run(cmd, capture_output=True, timeout=30).returncode
        except Exception:  # noqa: BLE001
            rc = 1
        if rc == 0:
            _cast_capture_mode = mode
            return mode
    return None


def _cast_ffmpeg_cmd(mode, outdir, source="screen"):
    gop = CAST_FPS * CAST_SEGMENT_SECONDS
    cmd = [FFMPEG, "-hide_banner", "-loglevel", "warning", "-nostats",
           "-progress", "pipe:1"]
    if source == "test":
        # Colour bars with a running counter, generated here: something
        # unmistakably ours on the TV before the real wall ever goes out.
        cmd += ["-re", "-f", "lavfi", "-i", "testsrc=size=1920x1080:rate=%d" % CAST_FPS,
                "-vf", "format=nv12"]
    else:
        cmd += _cast_input_args(mode)
    # -an is the whole audio story: there is no track for the TV to play.
    cmd += ["-an", "-fps_mode", "cfr", "-r", str(CAST_FPS)]
    cmd += _venc_args(VIDEO_ENCODER)
    cmd += ["-maxrate", CAST_MAXRATE, "-bufsize", CAST_MAXRATE,
            # One keyframe per segment, exactly on the boundary.
            "-g", str(gop), "-keyint_min", str(gop),
            "-force_key_frames", "expr:gte(t,n_forced*%d)" % CAST_SEGMENT_SECONDS,
            "-f", "hls", "-hls_time", str(CAST_SEGMENT_SECONDS),
            "-hls_list_size", str(CAST_LIST_SIZE),
            "-hls_flags", "delete_segments+omit_endlist+independent_segments+temp_file",
            "-hls_segment_type", "mpegts",
            "-hls_segment_filename", os.path.join(outdir, "live%d.ts"),
            os.path.join(outdir, "live.m3u8")]
    return cmd


def _cast_read_progress(proc):
    """ffmpeg -progress on stdout: keep fps/speed for the status line."""
    try:
        for raw in proc.stdout:
            key, _, val = raw.decode("utf-8", "ignore").strip().partition("=")
            with _cast_lock:
                if _cast["proc"] is not proc:
                    continue
                if key == "fps":
                    try:
                        _cast["fps"] = float(val)
                    except ValueError:
                        pass
                elif key == "speed":
                    _cast["speed"] = val.strip()
    except Exception:  # noqa: BLE001
        pass


def _cast_log_tail(path, n=6):
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            lines = [l.rstrip() for l in f if l.strip()]
        return " | ".join(lines[-n:])[-600:]
    except OSError:
        return ""


def _cast_spawn(outdir, source="screen"):
    if source == "test":
        mode = "test"
    else:
        mode = _cast_probe_capture()
        if not mode:
            raise CastRefused("this ffmpeg cannot capture the screen (no ddagrab/gdigrab). "
                              "Install a full build (winget install Gyan.FFmpeg) and restart.")
    log = os.path.join(outdir, "ffmpeg.log")
    errf = open(log, "w", encoding="utf-8")
    proc = subprocess.Popen(_cast_ffmpeg_cmd(mode, outdir, source), stdout=subprocess.PIPE,
                            stderr=errf, stdin=subprocess.DEVNULL)
    try:
        os.makedirs(MS_CACHE_HOME, exist_ok=True)
        with open(CAST_PID_FILE, "w", encoding="utf-8") as f:
            f.write(str(proc.pid))
    except OSError:
        pass
    threading.Thread(target=_cast_read_progress, args=(proc,), daemon=True).start()
    return proc, mode, log


def _cast_wait_playlist(outdir, proc):
    """Until the playlist lists CAST_WARMUP_SEGMENTS segments — or ffmpeg dies."""
    m3u8 = os.path.join(outdir, "live.m3u8")
    deadline = time.time() + CAST_START_TIMEOUT
    while time.time() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with open(m3u8, encoding="utf-8") as f:
                if sum(1 for l in f if l.strip().endswith(".ts")) >= CAST_WARMUP_SEGMENTS:
                    return True
        except OSError:
            pass
        time.sleep(0.25)
    return False


def _cast_kill(proc):
    if not proc:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:  # noqa: BLE001
        try:
            proc.kill()
        except Exception:  # noqa: BLE001
            pass
    try:
        os.remove(CAST_PID_FILE)
    except OSError:
        pass


def _cast_reap_orphan():
    """An ffmpeg left behind by a crashed server would keep grabbing the
    screen forever. Kill it — after checking the PID still IS an ffmpeg."""
    pid = None
    try:
        with open(CAST_PID_FILE, encoding="utf-8") as f:
            pid = int(f.read().strip())
    except (OSError, ValueError):
        pass
    if pid:
        try:
            if os.name == "nt":
                out = subprocess.run(["tasklist", "/FI", "PID eq %d" % pid, "/FO", "CSV", "/NH"],
                                     capture_output=True, text=True, timeout=10).stdout
                if "ffmpeg" in out.lower():
                    subprocess.run(["taskkill", "/PID", str(pid), "/F"],
                                   capture_output=True, timeout=10)
            else:
                with open("/proc/%d/cmdline" % pid, "rb") as f:
                    if b"ffmpeg" in f.read():
                        os.kill(pid, 15)
        except Exception:  # noqa: BLE001
            pass
        try:
            os.remove(CAST_PID_FILE)
        except OSError:
            pass
    for d in glob.glob(os.path.join(tempfile.gettempdir(), "multiscreen_cast_*")):
        shutil.rmtree(d, ignore_errors=True)


# --- the LAN listener ---------------------------------------------------------


class CastHandler(BaseHTTPRequestHandler):
    """Serves the live HLS to the paired TV, and to nobody else."""

    protocol_version = "HTTP/1.1"
    _path_re = re.compile(r"^/cast/(live\.m3u8|live\d+\.ts)$")

    def log_message(self, fmt, *args):
        # One line per second forever would be noise, so only while the cast
        # is starting — that is when "what did the TV ask for?" matters.
        with _cast_lock:
            starting = _cast["state"] == "starting"
        if starting:
            sys.stderr.write("  [cast] %s %s\n" % (self.client_address[0], fmt % args))

    def handle_one_request(self):
        # A renderer that drops a transfer and reconnects is normal.
        try:
            super().handle_one_request()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            self.close_connection = True

    def do_HEAD(self):
        self._serve(head=True)

    def do_GET(self):
        self._serve(head=False)

    def _deny(self, code):
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()

    def _serve(self, head):
        with _cast_lock:
            dev, outdir, state = _cast["device"], _cast["dir"], _cast["state"]
        client = self.client_address[0]
        if (not dev or client != dev["ip"] or state not in ("starting", "casting", "reconnecting")
                or not _cast_client_mac_ok(client, dev["mac"])):
            sys.stderr.write("  [cast] refused %s %s\n" % (client, self.path))
            return self._deny(403)
        m = self._path_re.match(self.path.split("?", 1)[0])
        if not m or not outdir:
            return self._deny(404)
        name = m.group(1)
        path = os.path.join(outdir, name)
        try:
            size = os.path.getsize(path)
            f = open(path, "rb")
        except OSError:
            return self._deny(404)
        with f:
            start, end, status = 0, size - 1, 200
            rng = self.headers.get("Range")
            if rng:
                rm = re.match(r"bytes=(\d*)-(\d*)", rng)
                if rm:
                    start = int(rm.group(1) or 0)
                    end = int(rm.group(2)) if rm.group(2) else size - 1
                    end = min(end, size - 1)
                    if start > end:
                        return self._deny(416)
                    status = 206
            self.send_response(status)
            if name.endswith(".m3u8"):
                self.send_header("Content-Type", "application/vnd.apple.mpegurl")
                self.send_header("Cache-Control", "no-cache, no-store")
            else:
                self.send_header("Content-Type", "video/mp2t")
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Accept-Ranges", "bytes")
            if status == 206:
                self.send_header("Content-Range", "bytes %d-%d/%d" % (start, end, size))
            self.send_header("transferMode.dlna.org", "Streaming")
            self.send_header("contentFeatures.dlna.org", DLNA_FEATURES)
            self.end_headers()
            if not head:
                f.seek(start)
                left = end - start + 1
                while left > 0:
                    chunk = f.read(min(65536, left))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    left -= len(chunk)
        with _cast_lock:
            if _cast["first_fetch"] is None:
                _cast["first_fetch"] = time.time()
            if name.endswith(".ts") and not head:
                _cast["served"] += 1
                if _cast["anchor"] is None:
                    # The TV counts its position from the first segment it
                    # takes; remembering when that one was written turns its
                    # RelTime into "how far behind the screen am I".
                    try:
                        _cast["anchor"] = (int(re.sub(r"\D", "", name)), os.path.getmtime(path))
                    except (ValueError, OSError):
                        pass


def _cast_client_mac_ok(ip, mac):
    """Does the ARP table say `ip` is the paired MAC? Cached briefly: the TV
    asks for a segment every second, `arp` is a process."""
    now = time.time()
    hit = _cast_mac_cache.get(ip)
    if not hit or now - hit[1] > CAST_MAC_CACHE_SECONDS:
        hit = (_cast_mac_for(ip), now)
        _cast_mac_cache[ip] = hit
    return hit[0] is not None and hit[0] == mac


def _cast_lan_ip(target_ip):
    """Our address on the interface that reaches the TV (no packet is sent)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((target_ip, 9))
        return s.getsockname()[0]
    finally:
        s.close()


def _cast_ensure_server(lan_ip):
    """The listener, bound to `lan_ip` only. Rebinds if the interface changed."""
    global _cast_server
    if _cast_server and _cast_server[1] == lan_ip:
        return _cast_server[2]
    if _cast_server:
        try:
            _cast_server[0].shutdown()
            _cast_server[0].server_close()
        except Exception:  # noqa: BLE001
            pass
        _cast_server = None
    base = (PROXY_PORTS[0] if PROXY_PORTS else 8000) + CAST_PORT_OFFSET
    last = None
    for port in range(base, base + 10):
        try:
            httpd = Server((lan_ip, port), CastHandler)
        except OSError as exc:
            last = exc
            continue
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        _cast_server = (httpd, lan_ip, port)
        return port
    raise CastRefused("could not open a port for the cast listener (%s)" % last)


# --- the session ----------------------------------------------------------------


def cast_status():
    with _cast_lock:
        s = dict(_cast)
    casting = s["state"] in ("casting", "reconnecting") and s["started"]
    return {
        "ok": True,
        "state": s["state"],
        "source": s["source"],
        "checks": list(s["checks"]),
        "owner_marks": list(CAST_OWNER_MARKS),
        "require_test_minutes": CAST_REQUIRE_TEST_MINUTES,
        "test_ok_until": (_cast_last_test["at"] + CAST_REQUIRE_TEST_MINUTES * 60)
                         if _cast_last_test else None,
        "test_ok_for": _cast_last_test["uuid"] if _cast_last_test else None,
        "latency": round(s["latency"], 1) if s["latency"] is not None else None,
        "reconnect_since": s["reconnect_since"],
        "paired": _cast_load_device(),
        "device": s["device"],
        "url": s["url"],
        "tv": {"state": s["tv_state"], "position": s["tv_pos"]},
        "encoder": s["encoder"], "capture": s["capture"],
        "fps": s["fps"], "speed": s["speed"],
        "served": s["served"],
        "reached": s["first_fetch"] is not None,
        "uptime": (time.time() - s["started"]) if casting else 0,
        "restarts": s["restarts"], "repushes": s["repushes"],
        "warning": s["warning"], "error": s["error"],
    }


def _cast_teardown(error=None):
    """Stop everything: tell the TV, kill ffmpeg, drop the segments."""
    with _cast_lock:
        proc, dev, outdir, was = _cast["proc"], _cast["device"], _cast["dir"], _cast["state"]
        _cast.update(state="error" if error else "idle", proc=None, url=None, dir=None,
                     error=error, fps=None, speed=None, started=None,
                     tv_state=None, tv_pos=None, anchor=None, latency=None,
                     reconnect_since=None)
    if dev and was in ("casting", "starting", "reconnecting"):
        try:
            _cast_soap(dev["control"], "Stop")
        except Exception:  # noqa: BLE001
            pass
        _cast_audit("stop", dev, reason=error)
    _cast_kill(proc)
    if outdir:
        shutil.rmtree(outdir, ignore_errors=True)


def cast_stop():
    _cast_teardown()
    return cast_status()


def cast_start(source="screen"):
    """Start casting the screen — or, with source="test", the colour-bar
    pattern: same lock, same listener, same push, so a pattern showing up on
    the right TV proves the whole path before any real picture goes out."""
    with _cast_lock:
        if _cast["state"] in ("starting", "casting", "reconnecting"):
            return cast_status()
        _cast.update(state="starting", source=source, error=None, warning=None, url=None,
                     served=0, first_fetch=None, restarts=0, repushes=0,
                     fps=None, speed=None, tv_state=None, tv_pos=None,
                     anchor=None, latency=None, reconnect_since=None)
    try:
        if not FFMPEG:
            raise CastRefused("ffmpeg not found on the server. Install it (winget install "
                              "Gyan.FFmpeg, or pip install imageio-ffmpeg) and restart.")
        target = cast_verify_target()
        if source == "screen" and CAST_REQUIRE_TEST_MINUTES > 0:
            last = _cast_last_test
            fresh = (last and last["uuid"] == target["uuid"] and last["mac"] == target["mac"]
                     and time.time() - last["at"] < CAST_REQUIRE_TEST_MINUTES * 60)
            if not fresh:
                _cast_audit("refused", target, reason="no fresh test pattern")
                raise CastRefused("test pattern first: a screen cast needs a test pattern that "
                                  "played on %s within the last %d min%s." % (
                                      target["name"], CAST_REQUIRE_TEST_MINUTES,
                                      " (none since this server started)" if not last
                                      else " (the last one ended %d min ago)"
                                      % int((time.time() - last["at"]) // 60)))
            with _cast_lock:
                _cast["checks"].append("test pattern played here %d min ago"
                                       % int((time.time() - last["at"]) // 60))
        lan_ip = _cast_lan_ip(target["ip"])
        port = _cast_ensure_server(lan_ip)
        outdir = tempfile.mkdtemp(prefix="multiscreen_cast_")
        with _cast_lock:
            _cast.update(device=target, dir=outdir)
        proc, mode, log = _cast_spawn(outdir, source)
        with _cast_lock:
            _cast.update(proc=proc, capture=mode, encoder=VIDEO_ENCODER, log=log)
        if not _cast_wait_playlist(outdir, proc):
            tail = _cast_log_tail(log)
            raise CastRefused("the encoder did not start" + (": " + tail if tail else "."))
        url = "http://%s:%d/cast/live.m3u8" % (lan_ip, port)
        _cast_push(target, url)
        with _cast_lock:
            _cast.update(state="casting", url=url, started=time.time())
        threading.Thread(target=_cast_monitor, args=(proc,), daemon=True).start()
        sys.stderr.write("  [cast] %s (%s) <- %s via %s/%s [%s]\n"
                         % (target["name"], target["mac"], url, mode, VIDEO_ENCODER, source))
    except CastRefused as exc:
        _cast_teardown(error=str(exc))
        raise
    except Exception as exc:  # noqa: BLE001
        _cast_teardown(error="cast failed: %s" % exc)
        raise
    return cast_status()


def _cast_reltime(text):
    """'0:01:02.345' -> 62.345; None when the TV sends nothing usable."""
    try:
        h, m, s = text.strip().split(":")
        return int(h) * 3600 + int(m) * 60 + float(s)
    except (AttributeError, ValueError):
        return None


def _cast_monitor(proc):
    """While casting: keep ffmpeg alive, ask the TV what it is doing, push
    again if it stopped, wait for it if it goes away, estimate how far behind
    the screen it is, and say so when it never came for the stream."""
    stopped_polls = 0
    unreachable = 0
    last_repush = 0.0
    last_knock = 0.0
    while True:
        time.sleep(CAST_POLL_SECONDS)
        with _cast_lock:
            if _cast["state"] not in ("casting", "reconnecting") or _cast["proc"] is not proc:
                return
            snap = dict(_cast)

        # 1. The encoder. Restart it in a fresh directory (segment numbers
        #    start over, so the TV needs the URL handed to it again).
        if proc.poll() is not None:
            tail = _cast_log_tail(snap["log"])
            if snap["restarts"] >= CAST_MAX_RESTARTS:
                _cast_teardown(error="the encoder keeps dying" + (": " + tail if tail else "."))
                return
            try:
                newdir = tempfile.mkdtemp(prefix="multiscreen_cast_")
                newproc, _mode, newlog = _cast_spawn(newdir, snap["source"])
                if not _cast_wait_playlist(newdir, newproc):
                    raise CastRefused("encoder restart failed: " + _cast_log_tail(newlog))
                with _cast_lock:
                    _cast.update(proc=newproc, dir=newdir, log=newlog, anchor=None, latency=None)
                    _cast["restarts"] += 1
                    _cast["warning"] = "the encoder died and was restarted" + (" (" + tail + ")" if tail else "")
                shutil.rmtree(snap["dir"], ignore_errors=True)
                if snap["state"] == "casting":
                    _cast_push(snap["device"], snap["url"])
                proc = newproc
            except Exception as exc:  # noqa: BLE001
                _cast_teardown(error=str(exc))
                return
            continue

        # 2. The TV is away (off, rebooting, on another input). Keep the
        #    encoder warm and knock every few seconds — through the same
        #    lock as any push — until it is back or the budget runs out.
        if snap["state"] == "reconnecting":
            if time.time() - snap["reconnect_since"] > CAST_RECONNECT_BUDGET:
                _cast_teardown(error="the TV did not come back within %d minutes"
                               % (CAST_RECONNECT_BUDGET // 60))
                return
            if time.time() - last_knock < CAST_RECONNECT_EVERY:
                continue
            last_knock = time.time()
            try:
                target = cast_verify_target()
                _cast_push(target, snap["url"])
            except CastAbsent:
                continue
            except CastRefused as exc:
                _cast_teardown(error=str(exc))
                return
            except Exception as exc:  # noqa: BLE001
                with _cast_lock:
                    _cast["warning"] = "the TV is back but the push failed: %s" % exc
                continue
            away = int(time.time() - snap["reconnect_since"])
            with _cast_lock:
                _cast.update(state="casting", device=target, reconnect_since=None,
                             anchor=None, latency=None, tv_state=None, tv_pos=None)
                _cast["repushes"] += 1
                _cast["warning"] = "the TV was away for %ds - the stream was pushed again" % away
            unreachable = stopped_polls = 0
            continue

        # 3. Accepted but never fetched: the packets are not arriving.
        if snap["first_fetch"] is None and time.time() - snap["started"] > CAST_FIRST_FETCH_GRACE:
            port = urllib.parse.urlparse(snap["url"]).port
            with _cast_lock:
                _cast["warning"] = ("the TV accepted the stream but never asked for it — "
                                    "a firewall on this PC is probably blocking TCP port %d "
                                    "for python.exe" % port)

        # 4. Ask the TV. Two misses in a row and it is considered away.
        try:
            st, pos = _cast_tv_state(snap["device"]["control"])
        except Exception:  # noqa: BLE001
            unreachable += 1
            if unreachable >= CAST_UNREACHABLE_POLLS:
                with _cast_lock:
                    _cast.update(state="reconnecting", reconnect_since=time.time(),
                                 tv_state=None, tv_pos=None,
                                 warning="the TV stopped answering - waiting for it to come back")
                last_knock = 0.0
            continue
        unreachable = 0

        # 5. Latency: the TV counts RelTime from the first segment it took,
        #    and we know when that segment was written. Smoothed, because the
        #    TV reports in steps and drifts a little.
        rel = _cast_reltime(pos) if pos else None
        if snap["source"] == "test" and st == "PLAYING" and snap["served"] > 0:
            # The pattern is on that screen: this TV, this UUID+MAC, now.
            global _cast_last_test
            _cast_last_test = {"uuid": snap["device"]["uuid"], "mac": snap["device"]["mac"],
                               "at": time.time()}
        with _cast_lock:
            _cast["tv_state"], _cast["tv_pos"] = st, pos
            anchor = _cast["anchor"]
            if anchor and rel is not None and st == "PLAYING":
                shown_at = anchor[1] - CAST_SEGMENT_SECONDS + rel   # when what is on the TV now was captured
                est = time.time() - shown_at
                if 0.0 < est < 120.0:
                    old = _cast["latency"]
                    _cast["latency"] = est if old is None else old * 0.7 + est * 0.3

        # 6. The TV stopped on its own: hand the stream over again.
        stopped_polls = stopped_polls + 1 if st in ("STOPPED", "NO_MEDIA_PRESENT") else 0
        if stopped_polls >= 2 and time.time() - last_repush > CAST_REPUSH_COOLDOWN:
            stopped_polls = 0
            last_repush = time.time()
            try:
                # The lock applies to every push, including this one.
                target = cast_verify_target()
                _cast_push(target, snap["url"])
                with _cast_lock:
                    _cast.update(device=target, anchor=None, latency=None)
                    _cast["repushes"] += 1
                    _cast["warning"] = "the TV had stopped - the stream was pushed again"
            except CastAbsent:
                with _cast_lock:
                    _cast.update(state="reconnecting", reconnect_since=time.time(),
                                 warning="the TV went away - waiting for it to come back")
                last_knock = time.time()
            except CastRefused as exc:
                _cast_teardown(error=str(exc))
                return
            except Exception as exc:  # noqa: BLE001
                with _cast_lock:
                    _cast["warning"] = "re-push failed: %s" % exc


def cast_selftest():
    """Prove the lock on the live network: the paired TV passes, every other
    renderer is refused. Exit code 0 = PASS. Run: python server.py --cast-selftest"""
    paired = _cast_load_device()
    print("Cast self-test")
    print("  paired: %s" % (("%s (%s)  uuid=%s  mac=%s" % (paired["name"], paired["model"],
                            paired["uuid"], paired["mac"])) if paired else "none"))
    devices = cast_discover(4.0)
    print("  renderers answering now: %d" % len(devices))
    for d in devices:
        print("    - %-18s %-14s %-16s %-18s %s" % (d["name"], d["model"], d["ip"],
                                                   d["mac"] or "(no MAC)", d["uuid"]))
    if not paired:
        print("FAIL: no TV paired - pair one in the app first.")
        return 1
    ok = True
    print("  owner marks: %s   (a name without one can be neither paired nor cast to)"
          % list(CAST_OWNER_MARKS))
    if CAST_OWNER_MARKS and not _cast_owner_mark(paired["name"]):
        print("FAIL: the paired TV's own name %r carries no owner mark" % paired["name"])
        ok = False
    try:
        t = cast_verify_target()
        print("PASS: would cast to %s (%s, %s)" % (t["name"], t["ip"], t["mac"]))
    except CastRefused as exc:
        print("FAIL: paired TV not accepted - %s" % exc)
        ok = False
    others = [d for d in devices if d["uuid"] != paired["uuid"]]
    for d in others:
        why = _cast_check(paired, d)
        if why:
            print("PASS: %s (%s) refused - %s" % (d["name"], d["ip"], why))
        else:
            print("FAIL: %s (%s) would be ACCEPTED" % (d["name"], d["ip"]))
            ok = False
    if not others:
        print("  note: no other renderer was on the network to be refused this time")
    print("RESULT: %s" % ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


# ---------- diagnosis helpers ----------

def _safe_url(u):
    """host + path, no query: signed CDN queries are credentials."""
    p = urllib.parse.urlsplit(u)
    path = p.path if len(p.path) <= 78 else p.path[:38] + "..." + p.path[-37:]
    return "%s%s%s" % (p.netloc, path, "?<query>" if p.query else "")


def _query_keys(u):
    q = urllib.parse.urlsplit(u).query
    if not q:
        return ""
    return ", ".join(k for k, _ in urllib.parse.parse_qsl(q, keep_blank_values=True))


def _status(st, err):
    if err:
        return "ERROR %s" % err[:90]
    return "HTTP %d%s" % (st, "" if st == 200 else "   <-- refused")


def _attempt_variants(child, parent_query):
    """The forms a playlist child URI can take on a signed CDN, in order."""
    own = urllib.parse.urlsplit(child).query
    tries = [("as-listed", child)]
    if not own and parent_query:
        p = urllib.parse.urlsplit(child)
        tries.append(("+parent query", urllib.parse.urlunsplit(
            (p.scheme, p.netloc, p.path, parent_query, p.fragment))))
    if own:
        p = urllib.parse.urlsplit(child)
        tries.append(("query stripped", urllib.parse.urlunsplit(
            (p.scheme, p.netloc, p.path, "", p.fragment))))
    return tries


def _probe(url, headers, impersonated):
    """GET `url`, returning (status, content_type, body[:64k], error)."""
    hdrs = {k: v for k, v in (headers or {}).items() if v}
    hdrs.setdefault("User-Agent", DEFAULT_UA)
    hdrs.setdefault("Accept-Encoding", "identity")
    try:
        up = open_upstream(url, hdrs, impersonated, attempts=1)
    except urllib.error.HTTPError as e:
        return e.code, "", b"", ""
    except Exception as e:  # noqa: BLE001
        return 0, "", b"", str(e)
    try:
        ctype = up.header("Content-Type", "?")
        chunks, got = [], 0
        for c in up.chunks():
            chunks.append(c)
            got += len(c)
            if got >= 65536:
                break
        return up.status, ctype, b"".join(chunks), ""
    finally:
        up.close()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        # Compact console log.
        sys.stderr.write("  %s\n" % (fmt % args))

    # ---------- routing ----------

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if path == "/api/resolve":
            return self.handle_resolve(qs)
        if path == "/api/scan":
            return self.handle_scan(qs)
        if path == "/api/wrap":
            return self.handle_wrap(qs)
        if path == "/api/proxy":
            return self.handle_proxy(qs)
        if path == "/api/diagnose":
            return self.handle_diagnose(qs)
        if path == "/api/compile/status":
            return self.handle_compile_status(qs)
        if path == "/api/analyze/status":
            return self.handle_analyze_status(qs)
        if path == "/api/analyze/result":
            return self.handle_analyze_result(qs)
        if path == "/api/clip/status":
            return self.handle_clip_status(qs)
        if path == "/api/clip/index/status":
            return self.handle_clip_index_status(qs)
        if path == "/api/find/status":
            return self.handle_find_status(qs)
        if path == "/api/channel/status":
            return self.handle_channel_status(qs)
        if path == "/api/shrink/status":
            return self.handle_shrink_status(qs)
        if path == "/api/compile/result":
            return self.handle_compile_result(qs)
        if path == "/api/voices":
            return self.handle_voices(qs)
        if path == "/api/tts":
            return self.handle_tts(qs)
        if path == "/api/cast/status":
            return self.handle_cast_status(qs)
        if path == "/api/cast/devices":
            return self.handle_cast_devices(qs)
        if path.startswith("/api/localfile/"):
            return self.handle_localfile(path)
        return self.handle_static(path)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/api/compile":
            return self.handle_compile()
        if parsed.path == "/api/analyze":
            return self.handle_analyze()
        if parsed.path == "/api/clip/setup":
            return self.handle_clip_setup()
        if parsed.path == "/api/clip/index":
            return self.handle_clip_index()
        if parsed.path == "/api/clip/search":
            return self.handle_clip_search()
        if parsed.path == "/api/clip/similar":
            return self.handle_clip_similar()
        if parsed.path == "/api/channel/list":
            return self.handle_channel_list()
        if parsed.path == "/api/channel/compile":
            return self.handle_channel_compile()
        if parsed.path == "/api/upload":
            return self.handle_upload(qs)
        if parsed.path == "/api/shrink":
            return self.handle_shrink(qs)
        if parsed.path == "/api/find":
            return self.handle_find()
        if parsed.path == "/api/related":
            return self.handle_related()
        if parsed.path == "/api/cast/pair":
            return self.handle_cast_pair()
        if parsed.path == "/api/cast/unpair":
            return self.handle_cast_unpair()
        if parsed.path == "/api/cast/start":
            return self.handle_cast_start()
        if parsed.path == "/api/cast/stop":
            return self.handle_cast_stop()
        return self.send_error(404, "Not found")

    def do_OPTIONS(self):
        # CORS preflight: hls.js on the main port fetches segments from the
        # shard ports with a Range header, which triggers a preflight.
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Range, Origin, Accept, Content-Type")
        self.send_header("Access-Control-Max-Age", "86400")
        self.send_header("Content-Length", "0")
        self.end_headers()

    # ---------- static ----------

    def handle_static(self, path):
        if path == "/" or path == "":
            path = "/index.html"
        # Blocks path traversal.
        safe = os.path.normpath(path).lstrip("\\/")
        full = os.path.join(ROOT, safe)
        if not full.startswith(ROOT) or not os.path.isfile(full):
            return self.send_error(404, "Not found")
        ext = os.path.splitext(full)[1].lower()
        try:
            with open(full, "rb") as f:
                data = f.read()
        except OSError:
            return self.send_error(404, "Not found")
        self.send_response(200)
        self.send_header("Content-Type", MIME.get(ext, "application/octet-stream"))
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # ---------- /api/resolve ----------

    def request_host(self):
        """Hostname the browser used (localhost/127.0.0.1), without the port."""
        host = self.headers.get("Host") or "localhost"
        return host.rsplit(":", 1)[0]

    def proxy_url(self, target, headers, impersonated=False, org=None,
                  qmax=None, sub=False, pq=""):
        """Absolute proxy URL on the next shard port (round-robin)."""
        token = urllib.parse.quote(encode_target(target, headers, impersonated,
                                                 org=org, qmax=qmax, sub=sub,
                                                 pq=pq))
        return "http://%s:%d/api/proxy?p=%s" % (
            self.request_host(), next_proxy_port(), token)

    def scan_items(self, page_url, page_title, vids, used_imp):
        """Proxied entries for the videos found on a page (one per <video>)."""
        # Hotlink protection on these files usually checks the Referer; send
        # the page they were found on.
        media_headers = {"User-Agent": DEFAULT_UA, "Referer": page_url}
        items = []
        for i, v in enumerate(vids):
            name = v.split("?")[0].rsplit("/", 1)[-1] or ("video %d" % (i + 1))
            items.append({
                "url": self.proxy_url(v, media_headers, used_imp),
                "src": v,
                "isHls": is_hls(v),
                "title": (page_title + " · " if page_title else "") + name,
            })
        return items

    def handle_resolve(self, qs):
        """Resolve one link into something playable.

        With `scan=1` the page is also scanned for <video> elements *while*
        yt-dlp works, and the two race: a page hosting several videos becomes
        one tile per video, anything else falls back to the extracted stream.
        The scan is also the safety net when yt-dlp can't extract at all (JS
        walls, unsupported sites) — a plain <video> src often still plays.
        """
        url = (qs.get("url") or [""])[0].strip()
        if not url:
            return self.send_json({"ok": False, "error": "Empty URL"}, 400)
        # Quality cap (height in px). Defaults to 480p so many tiles can play
        # at once. q=0 (or negative) = no cap.
        try:
            qmax = int((qs.get("q") or ["480"])[0])
        except ValueError:
            qmax = 480
        want_scan = (qs.get("scan") or ["0"])[0] not in ("0", "", "false")
        # Origin of a blob: URL the user pasted next to this link: the host of
        # the frame that actually holds the player, so the scan opens that
        # iframe first instead of guessing.
        prefer_origin = (qs.get("origin") or [""])[0].strip()
        # A retry must not be answered from cache. Signed CDN links expire, and
        # handing the tile the same dead URL back is exactly why reconnecting
        # used to fail forever: every attempt replayed the cached failure.
        if (qs.get("fresh") or ["0"])[0] not in ("0", "", "false"):
            with _resolve_lock:
                _resolve_cache.pop((url, qmax), None)
            with _scan_lock:
                _scan_cache.pop((url, prefer_origin), None)

        deadline = time.time() + SCAN_BUDGET
        scan_future = (_SCAN_POOL.submit(scan_page_videos, url, prefer_origin)
                       if want_scan else None)

        resolved, error, status = None, None, 502
        try:
            resolved = resolve_stream(url, qmax)
        except ImportError:
            error, status = "yt-dlp is not installed (pip install yt-dlp)", 500
        except ResolveError as e:
            error, status = e.message, e.status
        except Exception as e:  # noqa: BLE001
            error = str(e)[:200]

        scan = None
        if scan_future is not None:
            try:
                scan = scan_future.result(timeout=max(0.0, deadline - time.time()))
            except Exception:  # noqa: BLE001 — the scan is optional by design
                scan = None
        vids = scan[1] if scan else []
        # Referer the found URLs expect — the player's iframe when the video
        # turned up inside one, otherwise the page itself.
        ref = scan[3] if scan else url

        # Several players on one page: give each its own tile.
        if len(vids) >= 2:
            return self.send_json({
                "ok": True,
                "title": scan[0],
                "videos": self.scan_items(ref, scan[0], vids, scan[2]),
            })

        if resolved:
            return self.send_json({
                "ok": True,
                "title": resolved["title"],
                "stream": self.proxy_url(resolved["stream_url"], resolved["headers"],
                                         resolved.get("impersonated", False),
                                         org=url, qmax=qmax),
                "isHls": resolved["isHls"],
            })

        # Extraction failed but the page exposed a <video> — play that.
        if vids:
            item = self.scan_items(ref, scan[0], vids, scan[2])[0]
            return self.send_json({
                "ok": True,
                "title": item["title"],
                "stream": item["url"],
                "isHls": item["isHls"],
            })

        return self.send_json({"ok": False, "error": error or "nothing found"}, status)

    # ---------- /api/scan (all <video> elements on a page) ----------

    def handle_scan(self, qs):
        """Scans a page for <video> elements and returns one entry per video,
        already proxied with the right Referer, so each can play in its own
        tile. The front-end uses this before falling back to yt-dlp."""
        url = (qs.get("url") or [""])[0].strip()
        if not url:
            return self.send_json({"ok": False, "error": "Empty URL"}, 400)
        prefer_origin = (qs.get("origin") or [""])[0].strip()
        try:
            page_title, vids, used_imp, ref = scan_page_videos(url, prefer_origin)
        except ResolveError as e:
            return self.send_json({"ok": False, "error": e.message}, e.status)
        except Exception as e:  # noqa: BLE001
            return self.send_json({"ok": False, "error": str(e)[:200]}, 502)

        return self.send_json({
            "ok": True, "title": page_title,
            "videos": self.scan_items(ref, page_title, vids, used_imp),
        })

    # ---------- /api/wrap (media URL found by the browser) ----------

    def handle_wrap(self, qs):
        """Wraps a media URL the browser found for us into a playable entry.

        A blob: URL can't be fetched by anyone but the tab that created it, but
        the extension watched that tab and knows the real .m3u8/.mp4 behind it.
        That URL still needs the site's Referer and CORS headers to play here,
        which is exactly what the proxy does — so all this does is mint the
        token. No network access, so it answers instantly.
        """
        url = (qs.get("url") or [""])[0].strip()
        if not url.lower().startswith(("http://", "https://")):
            return self.send_json(
                {"ok": False, "error": "Not an http(s) media URL"}, 400)
        ref = (qs.get("ref") or [""])[0].strip()
        title = (qs.get("title") or [""])[0].strip()
        item = self.scan_items(ref or url, title, [url], False)[0]
        item["ok"] = True
        return self.send_json(item)

    # ---------- /api/compile (async job with progress) ----------

    def handle_compile(self):
        """Start a compile job and return its id. The browser polls
        /api/compile/status for progress, then downloads /api/compile/result."""
        if not FFMPEG:
            return self.send_json({"ok": False, "error":
                "ffmpeg not found on the server. Install it with "
                "'pip install imageio-ffmpeg' (or add ffmpeg to PATH) and restart."}, 500)

        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        body = self.rfile.read(length) if length > 0 else b""
        try:
            data = json.loads(body.decode("utf-8"))
        except Exception:  # noqa: BLE001
            return self.send_json({"ok": False, "error": "invalid JSON body"}, 400)

        # Validate cuts: need a url and end > start.
        cuts = []
        for c in (data.get("cuts") or []):
            try:
                u = (c.get("url") or "").strip()
                s = max(0.0, float(c.get("start")))
                e = float(c.get("end"))
            except (TypeError, ValueError):
                continue
            if u and e > s:
                cuts.append({"url": u, "start": s, "end": e})
        if not cuts:
            return self.send_json(
                {"ok": False, "error": "no valid cuts (each needs a url and end > start)"}, 400)

        resolution = COMPILE_RES.get(str(data.get("resolution") or "720"), COMPILE_RES["720"])
        try:
            qmax = int(data.get("quality") or resolution[1])
        except (TypeError, ValueError):
            qmax = resolution[1]

        job_id = uuid.uuid4().hex
        now = time.time()
        with _compile_jobs_lock:
            # Opportunistically drop long-finished jobs.
            for jid in [k for k, v in _compile_jobs.items()
                        if now - v.get("ts", now) > COMPILE_JOB_TTL]:
                old = _compile_jobs.pop(jid, None)
                if old and old.get("tmpdir"):
                    shutil.rmtree(old["tmpdir"], ignore_errors=True)
            _compile_jobs[job_id] = {
                "stage": "queued", "done": 0, "total": len(cuts),
                "error": None, "result": None, "skipped": 0,
                "tmpdir": None, "ts": now,
            }
        threading.Thread(target=_run_compile_job,
                         args=(job_id, cuts, resolution, qmax), daemon=True).start()
        return self.send_json({"ok": True, "job_id": job_id, "total": len(cuts)})

    def handle_compile_status(self, qs):
        job_id = (qs.get("id") or [""])[0]
        with _compile_jobs_lock:
            job = _compile_jobs.get(job_id)
            snap = dict(job) if job else None
        if not snap:
            return self.send_json({"ok": False, "error": "unknown or expired job"}, 404)
        return self.send_json({
            "ok": True,
            "stage": snap["stage"],
            "done": snap["done"],
            "total": snap["total"],
            "skipped": snap["skipped"],
            "error": snap["error"],
            "ready": snap["stage"] == "done",
        })

    def handle_compile_result(self, qs):
        job_id = (qs.get("id") or [""])[0]
        with _compile_jobs_lock:
            job = _compile_jobs.get(job_id)
            snap = dict(job) if job else None
        if not snap or snap["stage"] != "done" or not snap.get("result"):
            return self.send_error(404, "result not ready")
        out = snap["result"]
        if not os.path.isfile(out):
            return self.send_error(404, "result missing")

        size = os.path.getsize(out)
        self.send_response(200)
        self.send_header("Content-Type", "video/mp4")
        self.send_header("Content-Length", str(size))
        self.send_header("Content-Disposition", 'attachment; filename="compilation.mp4"')
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Expose-Headers", "X-Compile-Skipped")
        self.send_header("X-Compile-Skipped", str(snap["skipped"]))
        self.end_headers()
        try:
            with open(out, "rb") as f:
                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            return
        # Download succeeded: drop the job and its scratch files.
        with _compile_jobs_lock:
            _compile_jobs.pop(job_id, None)
        if snap.get("tmpdir"):
            shutil.rmtree(snap["tmpdir"], ignore_errors=True)

    # ---------- /api/analyze (where the interesting moments are) ----------

    def handle_analyze(self):
        """Start a measuring pass over one video and return its job id.

        A curve already on disk comes straight back in this response, so the
        common case — reopening the Moments panel — makes no job at all.
        """
        if not FFMPEG:
            return self.send_json({"ok": False, "error":
                "ffmpeg not found on the server. Install it with "
                "'pip install imageio-ffmpeg' (or add ffmpeg to PATH) and restart."}, 500)

        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        body = self.rfile.read(length) if length > 0 else b""
        try:
            data = json.loads(body.decode("utf-8"))
        except Exception:  # noqa: BLE001
            return self.send_json({"ok": False, "error": "invalid JSON body"}, 400)

        url = (data.get("url") or "").strip()
        if not url:
            return self.send_json({"ok": False, "error": "no url"}, 400)
        try:
            qmax = int(data.get("quality") or 0)
        except (TypeError, ValueError):
            qmax = 0
        try:
            dur_hint = float(data.get("duration") or 0)
        except (TypeError, ValueError):
            dur_hint = 0.0

        if not data.get("fresh"):
            cached = _analyze_cache_get(url, qmax)
            # Curves measured before the coverage check existed can describe the
            # first six seconds of an eight minute video, and every consumer
            # then treats that as the video's length. The browser knows better.
            if cached and dur_hint and cached.get("duration", 0) < dur_hint * 0.9:
                sys.stderr.write("  ANALYZE cached curve is %s but the player reports %s"
                                 " - measuring again\n"
                                 % (_hms(cached.get("duration", 0)), _hms(dur_hint)))
                cached = None
            if cached:
                return self.send_json({"ok": True, "cached": True, "data": cached})
        # A probe only asks whether the curve already exists: opening the panel
        # on a big wall must not kick off forty decodes on its own.
        if data.get("probe"):
            return self.send_json({"ok": True, "cached": False})

        job_id = uuid.uuid4().hex
        now = time.time()
        with _analyze_jobs_lock:
            for jid in [k for k, v in _analyze_jobs.items()
                        if now - v.get("ts", now) > ANALYZE_JOB_TTL]:
                _analyze_jobs.pop(jid, None)
            _analyze_jobs[job_id] = {
                "stage": "queued", "at": 0.0, "dur": dur_hint,
                "error": None, "data": None, "tmpdir": None, "ts": now,
            }
        threading.Thread(target=_run_analyze_job,
                         args=(job_id, url, qmax, dur_hint), daemon=True).start()
        return self.send_json({"ok": True, "job_id": job_id})

    def handle_analyze_status(self, qs):
        job_id = (qs.get("id") or [""])[0]
        with _analyze_jobs_lock:
            job = _analyze_jobs.get(job_id)
            snap = dict(job) if job else None
        if not snap:
            return self.send_json({"ok": False, "error": "unknown or expired job"}, 404)
        dur = snap.get("dur") or 0.0
        return self.send_json({
            "ok": True,
            "stage": snap["stage"],
            "at": round(snap.get("at") or 0.0, 1),
            "dur": round(dur, 1),
            "pct": round(min(1.0, (snap.get("at") or 0.0) / dur), 4) if dur else None,
            "error": snap["error"],
            "ready": snap["stage"] == "done",
        })

    def handle_analyze_result(self, qs):
        job_id = (qs.get("id") or [""])[0]
        with _analyze_jobs_lock:
            job = _analyze_jobs.get(job_id)
            snap = dict(job) if job else None
        if not snap or snap["stage"] != "done" or not snap.get("data"):
            return self.send_json({"ok": False, "error": "result not ready"}, 404)
        # The curve is cached on disk; the job has nothing left to keep.
        with _analyze_jobs_lock:
            _analyze_jobs.pop(job_id, None)
        return self.send_json({"ok": True, "data": snap["data"]})

    # ---------- /api/clip (finding a moment by describing it) ----------

    def _body_json(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length > 0 else b""
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _read_window(data, key="window"):
        """[from, to] as fractions of the video, or None for the whole thing."""
        w = data.get(key)
        try:
            a, b = float(w[0]), float(w[1])
        except (TypeError, ValueError, IndexError):
            return None
        a, b = max(0.0, min(1.0, a)), max(0.0, min(1.0, b))
        return None if b - a >= 0.999 or b <= a else (a, b)

    def handle_clip_status(self, qs):
        """What the text search can do right now: deps, models, provider."""
        np, ort = _clip_deps()
        model = clip_model_id((qs.get("model") or [CLIP_DEFAULT_MODEL])[0])
        missing = _clip_missing(model) if np else list(clip_model(model)["files"])
        models = []
        for mid, cfg in CLIP_MODELS.items():
            gone = _clip_missing(mid) if np else list(cfg["files"])
            models.append({
                "id": mid,
                "label": cfg["label"],
                "ready": not gone,
                "bytes": sum(cfg["files"][n][1] for n in gone),
                "total": sum(sz for _p, sz in cfg["files"].values()),
            })
        return self.send_json({
            "ok": True,
            "api": API_VERSION,
            "deps": bool(np and ort),
            "model": model,
            "ready": bool(np and ort and not missing),
            "missing": missing,
            "bytes": sum(clip_model(model)["files"][n][1] for n in missing),
            "models": models,
            "presets": [{"id": k, "label": v["label"], "window": v["window"],
                         "prefer": v.get("prefer", "best"), "gate": v.get("gate", 0.35),
                         "grow": v.get("grow", 0.5), "model": v.get("model", "")}
                        for k, v in CLIP_PRESETS.items()],
            "provider": _clip_provider or "",
            "gpu": bool(np and ort and
                        ({"CUDAExecutionProvider", "DmlExecutionProvider"}
                         & set(ort.get_available_providers()))),
            "setup": dict(_clip_setup),
        })

    def handle_clip_setup(self):
        """Download one model's files (once). Progress: /api/clip/status."""
        np, ort = _clip_deps()
        if not (np and ort):
            return self.send_json({"ok": False, "error":
                "text search needs numpy and onnxruntime: "
                "pip install numpy onnxruntime  (or onnxruntime-directml to use the GPU)"}, 503)
        data = self._body_json() or {}
        model = clip_model_id(data.get("model") or CLIP_DEFAULT_MODEL)
        if _clip_setup.get("stage") == "downloading":
            return self.send_json({"ok": True, "stage": "downloading"})
        if not _clip_missing(model):
            return self.send_json({"ok": True, "stage": "done"})
        threading.Thread(target=_clip_download, args=(model,), daemon=True).start()
        return self.send_json({"ok": True, "stage": "downloading"})

    def handle_clip_index(self):
        """Embed one video's frames, or report that it is already embedded."""
        if not FFMPEG:
            return self.send_json({"ok": False, "error": "ffmpeg not found on the server"}, 500)
        data = self._body_json()
        if data is None:
            return self.send_json({"ok": False, "error": "invalid JSON body"}, 400)
        url = (data.get("url") or "").strip()
        if not url:
            return self.send_json({"ok": False, "error": "no url"}, 400)

        model = clip_model_id(data.get("model") or CLIP_DEFAULT_MODEL)
        window = self._read_window(data)
        try:
            dur_hint = float(data.get("duration") or 0)
        except (TypeError, ValueError):
            dur_hint = 0.0

        if not data.get("fresh"):
            have = clip_index_load(url, model, window)
            # The browser knows how long the tile is. An index cached by an
            # older session that covers a fraction of it is worse than none,
            # and this is the last place able to notice.
            want = dur_hint * (window[1] - window[0]) if (dur_hint and window) else dur_hint
            if have is not None and want and len(have) / CLIP_FPS < want * 0.9:
                sys.stderr.write("  CLIP cached index is %s but %s was expected"
                                 " - reindexing\n"
                                 % (_hms(len(have) / CLIP_FPS), _hms(want)))
                have = None
            if have is not None:
                meta = _clip_meta_read(url, model, window) or {}
                return self.send_json({"ok": True, "cached": True, "count": int(len(have)),
                                       "offset": meta.get("offset", 0)})
        if data.get("probe"):
            return self.send_json({"ok": True, "cached": False})

        np, ort = _clip_deps()
        if not (np and ort):
            return self.send_json({"ok": False, "error":
                "text search needs numpy and onnxruntime installed"}, 503)
        if _clip_missing(model):
            return self.send_json({"ok": False, "error":
                "the %s model is not downloaded yet" % clip_model(model)["label"]}, 409)

        job_id = uuid.uuid4().hex
        now = time.time()
        with _clip_jobs_lock:
            for jid in [k for k, v in _clip_jobs.items()
                        if now - v.get("ts", now) > ANALYZE_JOB_TTL]:
                _clip_jobs.pop(jid, None)
            _clip_jobs[job_id] = {"stage": "queued", "at": 0, "count": 0,
                                  "error": None, "ts": now}
        threading.Thread(target=_run_clip_job,
                         args=(job_id, url, model, dur_hint, window), daemon=True).start()
        return self.send_json({"ok": True, "job_id": job_id})

    def handle_clip_index_status(self, qs):
        job_id = (qs.get("id") or [""])[0]
        with _clip_jobs_lock:
            job = _clip_jobs.get(job_id)
            snap = dict(job) if job else None
        if not snap:
            return self.send_json({"ok": False, "error": "unknown or expired job"}, 404)
        return self.send_json({
            "ok": True,
            "stage": snap["stage"],
            "at": snap["at"],
            "count": snap["count"],
            "error": snap["error"],
            "ready": snap["stage"] == "done",
        })

    def handle_clip_search(self):
        """Score every second of an indexed video against a few phrases."""
        data = self._body_json()
        if data is None:
            return self.send_json({"ok": False, "error": "invalid JSON body"}, 400)
        url = (data.get("url") or "").strip()
        preset = data.get("preset") if data.get("preset") in CLIP_PRESETS else None
        prompts = [str(p).strip() for p in (data.get("prompts") or []) if str(p).strip()][:12]
        if not url or not (prompts or preset):
            return self.send_json({"ok": False,
                                   "error": "need a url and a phrase or a preset"}, 400)

        model = clip_model_id(data.get("model") or CLIP_DEFAULT_MODEL)
        window = self._read_window(data)
        vecs = clip_index_load(url, model, window)
        if vecs is None:
            return self.send_json({"ok": False, "error": "this video is not indexed yet"}, 409)
        offset = (_clip_meta_read(url, model, window) or {}).get("offset", 0)
        try:
            if preset:
                scores = {CLIP_PRESETS[preset]["label"]: clip_classify(vecs, preset, model)}
            else:
                scores = clip_search(vecs, prompts, model)
        except Exception as e:  # noqa: BLE001
            return self.send_json({"ok": False, "error": str(e)[:200]}, 500)
        # Logged so a search that went wrong can be looked at afterwards: what
        # was asked, over how much video, and how strong the best second was.
        sys.stderr.write("  CLIP search [%s] %s of video: %s\n"
                         % (model, _hms(len(vecs) / CLIP_FPS),
                            ", ".join("%s=%.2f@%s" % (p, max(v), _hms(v.index(max(v))))
                                      for p, v in scores.items())))
        return self.send_json({"ok": True, "step": 1.0 / CLIP_FPS, "offset": offset,
                               "count": int(len(vecs)), "scores": scores})

    def handle_clip_similar(self):
        """Score one video against an example clip from another tile."""
        data = self._body_json()
        if data is None:
            return self.send_json({"ok": False, "error": "invalid JSON body"}, 400)
        url = (data.get("url") or "").strip()
        ref_url = (data.get("ref") or "").strip()
        if not url or not ref_url:
            return self.send_json({"ok": False, "error": "need a url and a ref"}, 400)

        model = clip_model_id(data.get("model") or CLIP_DEFAULT_MODEL)
        window = self._read_window(data)
        vecs = clip_index_load(url, model, window)
        # The example is judged whole: whatever window the wall is searching,
        # the thing being looked for is all of the example tile.
        ref = clip_index_load(ref_url, model, self._read_window(data, "refWindow"))
        offset = (_clip_meta_read(url, model, window) or {}).get("offset", 0)
        if vecs is None:
            return self.send_json({"ok": False, "error": "this video is not indexed yet"}, 409)
        if ref is None:
            return self.send_json({"ok": False, "error":
                                   "the example tile is not indexed yet"}, 409)
        span = None
        try:
            if data.get("end"):
                span = (int(data.get("start") or 0), int(data["end"]))
        except (TypeError, ValueError):
            span = None
        try:
            curve, quality = clip_like(vecs, ref, span)
        except Exception as e:  # noqa: BLE001
            return self.send_json({"ok": False, "error": str(e)[:200]}, 500)
        best = curve.index(max(curve)) if curve else 0
        sys.stderr.write("  CLIP like [%s] %s of video, best second %s, "
                         "gap %.3f (median %.3f -> peak %.3f)\n"
                         % (model, _hms(len(vecs) / CLIP_FPS), _hms(best),
                            quality["gap"], quality["median"], quality["peak"]))
        return self.send_json({"ok": True, "step": 1.0 / CLIP_FPS, "offset": offset,
                               "count": int(len(vecs)), "curve": curve,
                               "quality": quality})

    # ---------- /api/channel (a performer's whole page) ----------

    def handle_channel_list(self):
        """The videos on a model/channel page, without opening any of them."""
        data = self._body_json()
        if data is None:
            return self.send_json({"ok": False, "error": "invalid JSON body"}, 400)
        url = (data.get("url") or "").strip()
        if not url:
            return self.send_json({"ok": False, "error": "no url"}, 400)
        try:
            listing = channel_videos(url, data.get("limit") or CHANNEL_MAX)
        except Exception as e:  # noqa: BLE001
            return self.send_json({"ok": False,
                                   "error": str(e).splitlines()[-1][:250]}, 502)
        return self.send_json({"ok": True, "title": listing["title"],
                               "count": len(listing["videos"]),
                               "videos": listing["videos"]})

    def handle_channel_compile(self):
        """Start the whole thing: list, measure, pick, cut, join."""
        if not FFMPEG:
            return self.send_json({"ok": False, "error": "ffmpeg not found on the server"}, 500)
        data = self._body_json()
        if data is None:
            return self.send_json({"ok": False, "error": "invalid JSON body"}, 400)
        url = (data.get("url") or "").strip()
        if not url:
            return self.send_json({"ok": False, "error": "no url"}, 400)

        preset = data.get("preset") if data.get("preset") in CLIP_PRESETS else CLIP_DEFAULT_PRESET
        model = clip_model_id(data.get("model") or CLIP_DEFAULT_MODEL)
        np, ort = _clip_deps()
        if not (np and ort):
            return self.send_json({"ok": False, "error":
                "this needs numpy and onnxruntime installed on the server"}, 503)
        if _clip_missing(model):
            return self.send_json({"ok": False, "error":
                "the %s model is not downloaded yet" % clip_model(model)["label"]}, 409)

        resolution = str(data.get("resolution") or "720")
        try:
            limit = int(data.get("limit") or CHANNEL_MAX)
        except (TypeError, ValueError):
            limit = CHANNEL_MAX

        job_id = uuid.uuid4().hex
        now = time.time()
        with _channel_jobs_lock:
            for jid in [k for k, v in _channel_jobs.items()
                        if now - v.get("ts", now) > CHANNEL_JOB_TTL]:
                _channel_jobs.pop(jid, None)
            _channel_jobs[job_id] = {
                "stage": "queued", "total": 0, "done": 0, "found": 0, "clips": [],
                "current": "", "error": None, "missed": [], "ts": now,
                "cut_done": 0, "total_cuts": 0, "compile_id": None, "title": "",
            }
        try:
            gate = float(data["gate"]) if data.get("gate") not in (None, "") else None
        except (TypeError, ValueError):
            gate = None
        threading.Thread(target=_run_channel_job,
                         args=(job_id, url, limit, preset, model, resolution, gate),
                         daemon=True).start()
        return self.send_json({"ok": True, "job_id": job_id})

    def handle_channel_status(self, qs):
        job_id = (qs.get("id") or [""])[0]
        with _channel_jobs_lock:
            job = _channel_jobs.get(job_id)
            snap = dict(job) if job else None
        if not snap:
            return self.send_json({"ok": False, "error": "unknown or expired job"}, 404)
        snap.pop("ts", None)
        snap["ok"] = True
        snap["ready"] = snap["stage"] == "done"
        snap.pop("result", None)          # a server path is no use to the browser
        return self.send_json(snap)

    # ---------- /api/find (a name in, a wall of thumbnails out) ----------

    def handle_find(self):
        """Start a search for a name and return its job id."""
        data = self._body_json()
        if data is None:
            return self.send_json({"ok": False, "error": "invalid JSON body"}, 400)
        name = re.sub(r"\s+", " ", (data.get("name") or "")).strip()[:80]
        if len(name) < 2:
            return self.send_json({"ok": False, "error": "type a name first"}, 400)

        job_id = uuid.uuid4().hex
        now = time.time()
        with _find_jobs_lock:
            for jid in [k for k, v in _find_jobs.items()
                        if now - v.get("ts", now) > FIND_JOB_TTL]:
                _find_jobs.pop(jid, None)
            _find_jobs[job_id] = {
                "stage": "queued", "name": name, "items": [], "done": 0,
                "total": 0, "found": 0, "error": None, "ts": now,
            }
        threading.Thread(target=_run_find_job,
                         args=(job_id, name, bool(data.get("fresh"))),
                         daemon=True).start()
        return self.send_json({"ok": True, "job_id": job_id, "name": name})

    def handle_find_status(self, qs):
        """Whatever has landed since the browser's last poll. `since` is how
        many cards it already has, so a long search is never re-sent."""
        job_id = (qs.get("id") or [""])[0]
        try:
            since = max(0, int((qs.get("since") or ["0"])[0]))
        except ValueError:
            since = 0
        with _find_jobs_lock:
            job = _find_jobs.get(job_id)
            if not job:
                return self.send_json({"ok": False,
                                       "error": "unknown or expired search"}, 404)
            items = job["items"][since:]
            snap = {"stage": job["stage"], "done": job["done"],
                    "total": job["total"], "found": job["found"],
                    "error": job["error"], "name": job["name"]}
            job["ts"] = time.time()
        snap.update({"ok": True, "items": items, "next": since + len(items),
                     "ready": snap["stage"] in ("done", "error")})
        return self.send_json(snap)

    # ---------- /api/related (a tile like the ones already up) ----------

    def handle_related(self):
        """Suggest video pages resembling the wall's own tiles.

        Two waves. The first mines the pages the tiles came from, for both
        sibling video links and the site's own taxonomy links. Those labels are
        then ranked by how much of the wall they actually name — the performer,
        character or game that keeps coming back — and the second wave mines
        the index pages of the top ones, where every video is on-theme by the
        site's own filing rather than by a guess about words.

        Body: {seeds: [page urls to mine], have: [urls to exclude],
               titles: [titles already on screen], want: N}
        Answers {ok, items: [{url, title, score, reason, via}], themes, ...},
        best first."""
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        try:
            data = json.loads((self.rfile.read(length) if length > 0 else b"")
                              .decode("utf-8"))
        except Exception:  # noqa: BLE001
            return self.send_json({"ok": False, "error": "invalid JSON body"}, 400)

        seeds, seen_seed = [], set()
        for u in (data.get("seeds") or []):
            if not isinstance(u, str):
                continue
            u = u.strip()
            if not u.lower().startswith(("http://", "https://")):
                continue
            if _NON_PAGE_RE.search(u):
                continue          # a direct media link is not a page to mine
            key = _norm_url(u)
            if key in seen_seed:
                continue
            seen_seed.add(key)
            seeds.append(u)
        if not seeds:
            return self.send_json({"ok": False, "error":
                "no page links to work from — Related mines the pages the "
                "tiles came from, so it needs at least one tile added by its "
                "site URL (not a direct file, a local file or an embed)."}, 400)

        try:
            want = max(1, min(20, int(data.get("want") or 5)))
        except (TypeError, ValueError):
            want = 5

        # Pages already scraped are free, so mine every one of those and spend
        # the round trips on a few fresh ones. That is also what keeps repeated
        # clicks varied: the pool grows while the wall's exclusions grow with it.
        shapes = {u: _url_shape(u) for u in seeds}
        cached = [u for u in seeds if _related_pooled(u, shapes[u]) is not None]
        fresh = [u for u in seeds if _related_pooled(u, shapes[u]) is None]
        random.shuffle(fresh)
        picked = cached[:8] + fresh[:RELATED_SEED_FETCHES]

        errors, fetches = [], 0

        def mine(page_url, want_shape):
            """One page, tolerating failure: a dead seed must not take the
            request down when the others have plenty to offer."""
            try:
                return _mine_cached(page_url, want_shape)
            except ResolveError as e:
                errors.append("%s: %s" % (_safe_url(page_url), e.message))
            except Exception as e:  # noqa: BLE001
                errors.append("%s: %s" % (_safe_url(page_url), str(e)[:120]))
            return None, False

        # --- wave 1: the pages the tiles came from ---
        mined = {}
        if picked:
            with ThreadPoolExecutor(max_workers=len(picked)) as ex:
                futures = {ex.submit(mine, u, shapes[u]): u for u in picked}
                for fut in as_completed(futures):
                    page = futures[fut]
                    res, was_cached = fut.result()
                    if res is None:
                        continue
                    if not was_cached:
                        fetches += 1
                    mined[page] = res

        # --- what the wall is about ---
        # A word's weight is how many titles carry it: the name that keeps
        # coming back (a performer, a character, a game) outweighs the one that
        # appeared once, which is the whole point of "more like these".
        title_words = []
        for t in (data.get("titles") or []):
            if isinstance(t, str) and t.strip():
                title_words.append(_words(t))
        for u in seeds:
            title_words.append(_slug_words(u))
        weight = collections.Counter()
        for tw in title_words:
            weight.update(tw)
        wall_bigrams = set()
        for t in (data.get("titles") or []):
            if isinstance(t, str):
                wall_bigrams |= _bigrams(t)

        # --- the wall's own labels, ranked by how much of the wall they name ---
        tag_index = {}
        for page, (_vids, tags) in mined.items():
            for tg in tags:
                key = _norm_url(tg["url"])
                entry = tag_index.setdefault(key, {
                    "url": tg["url"], "kind": tg["kind"],
                    "label": tg["label"] or tg["name"],
                    "words": _words(tg["label"] or tg["name"]), "pages": set(),
                })
                entry["pages"].add(page)
                entry["shape"] = shapes.get(page, ("", ()))
        for tg in tag_index.values():
            # How many tiles this label actually names, then how many of the
            # mined pages agreed on it.
            tg["names"] = sum(1 for tw in title_words if tg["words"] & tw)
            tg["seen"] = len(tg["pages"])
        themes = sorted(tag_index.values(),
                        key=lambda t: (-t["names"], -t["seen"], t["label"]))
        themes = [t for t in themes if t["words"] and t["shape"][1]]

        # --- wave 2: the index pages of the top labels ---
        # Every video on "/tags/shadowheart/" is on-theme by the site's own
        # judgement, which beats anything a title-word heuristic can infer.
        on_theme = {}
        picked_themes = themes[:RELATED_TAG_FETCHES]
        if picked_themes:
            with ThreadPoolExecutor(max_workers=len(picked_themes)) as ex:
                futures = {ex.submit(mine, t["url"], t["shape"]): t
                           for t in picked_themes}
                for fut in as_completed(futures):
                    tg = futures[fut]
                    res, was_cached = fut.result()
                    if res is None:
                        continue
                    if not was_cached:
                        fetches += 1
                    for c in res[0]:
                        on_theme.setdefault(_norm_url(c["url"]), (c, tg))

        have = {_norm_url(u) for u in (data.get("have") or [])
                if isinstance(u, str)}
        have |= seen_seed

        # --- score everything mined ---
        best = {}

        def offer(cand, via, theme):
            key = _norm_url(cand["url"])
            if key in have:
                return
            title = cand["title"] or _pretty_slug(cand["url"])
            words = _words(title) | _slug_words(cand["url"])
            shared = sorted(words & set(weight), key=lambda w: -weight[w])
            score = sum(weight[w] for w in shared)
            pairs = _bigrams(title) & wall_bigrams
            score += 2 * len(pairs)
            if theme:
                # Filed under a label that names the wall — worth more than any
                # single word coincidence, and scaled by how much of the wall
                # that label covers.
                score += 3 * max(1, theme["names"])
                reason = 'filed under %s "%s"' % (theme["kind"], theme["label"])
            elif shared:
                reason = "shares " + ", ".join(shared[:3])
            else:
                reason = "same site"
            prev = best.get(key)
            if prev and prev["score"] >= score:
                return
            best[key] = {"url": cand["url"], "title": title, "score": score,
                         "reason": reason, "via": via}

        for page, (vids, _tags) in mined.items():
            for c in vids:
                key = _norm_url(c["url"])
                hit = on_theme.get(key)
                offer(c, page, hit[1] if hit else None)
        for key, (c, tg) in on_theme.items():
            offer(c, tg["url"], tg)

        items = list(best.values())
        # Shuffle first, then a stable sort: equal scores come back in a
        # different order every click instead of always the same tile.
        random.shuffle(items)
        items.sort(key=lambda c: -c["score"])
        return self.send_json({
            "ok": True,
            "items": items[:want],
            "pool": len(items),
            "seeds_used": len(mined),
            "fetched": fetches,
            "themes": [{"label": t["label"], "kind": t["kind"],
                        "names": t["names"]} for t in picked_themes],
            "error": errors[0] if errors and not items else None,
        })

    # ---------- /api/shrink (tile-sized re-encode) ----------

    def handle_shrink(self, qs):
        """Re-encode an uploaded video down to the size its tile shows.

        Takes an upload id (from /api/upload), starts a background encode and
        returns its job id; the browser polls /api/shrink/status and then plays
        the returned /api/localfile/ URL. A video already within the box is
        reported back untouched, so re-running this over a light package costs
        one probe per file and no encoding at all."""
        if not FFMPEG:
            return self.send_json({"ok": False, "error":
                "ffmpeg not found on the server. Install it with "
                "'pip install imageio-ffmpeg' (or add ffmpeg to PATH) and restart."}, 500)

        fid = (qs.get("id") or [""])[0]
        with _uploads_lock:
            src = _uploads.get(fid)
        if not src or not os.path.isfile(src):
            src = _recover_upload(fid)   # the registry may be a restart younger
        if not src:
            return self.send_json({"ok": False, "error": "unknown upload id"}, 404)

        def clamp(name, default, lo, hi):
            try:
                v = int(float((qs.get(name) or [str(default)])[0]))
            except (TypeError, ValueError):
                v = default
            return max(lo, min(hi, v))

        hbox = clamp("h", 432, 144, 2160)
        fps_cap = clamp("fps", SHRINK_FPS, 10, 120)
        drop = (qs.get("drop") or ["0"])[0] == "1"

        info = probe_media(src)
        if not info:
            return self.send_json(
                {"ok": False, "error": "could not read the video's format"}, 422)

        src_ext = os.path.splitext(src)[1] or ".mp4"
        origin = {"w": info["w"], "h": info["h"], "fps": round(info["fps"], 2),
                  "dur": round(info["dur"], 2), "codec": info["codec"],
                  "bytes": os.path.getsize(src)}

        _, tw, th = _shrink_filters(info, hbox, fps_cap)
        if not _shrink_needed(info, hbox, fps_cap):
            return self.send_json({
                "ok": True, "needed": False, "from": origin,
                "url": "/api/localfile/%s/file%s" % (fid, src_ext),
            })

        job_id = uuid.uuid4().hex
        now = time.time()
        with _shrink_jobs_lock:
            for jid in [k for k, v in _shrink_jobs.items()
                        if now - v.get("ts", now) > SHRINK_JOB_TTL]:
                _shrink_jobs.pop(jid, None)
            _shrink_jobs[job_id] = {
                "stage": "queued", "pct": 0.0, "error": None, "url": None,
                "from": origin, "to": {"w": tw, "h": th}, "bytes": 0,
                "encoder": None, "ts": now,
            }

        try:
            os.makedirs(_UPLOAD_DIR, exist_ok=True)
        except OSError:
            pass
        out = os.path.join(_UPLOAD_DIR, "shrink_%s.mp4" % job_id)

        # The queue is waited on inside the worker, not here: the HTTP request
        # must return the job id right away so the browser can show a row for
        # every file, including the ones still waiting for an encoder slot.
        def worker():
            with _shrink_slots:
                _run_shrink_job(job_id, src, out, info, hbox, fps_cap, drop)

        threading.Thread(target=worker, daemon=True).start()
        return self.send_json({"ok": True, "needed": True, "job_id": job_id,
                               "from": origin, "to": {"w": tw, "h": th}})

    def handle_shrink_status(self, qs):
        job_id = (qs.get("id") or [""])[0]
        with _shrink_jobs_lock:
            job = _shrink_jobs.get(job_id)
            snap = dict(job) if job else None
        if not snap:
            return self.send_json({"ok": False, "error": "unknown or expired job"}, 404)
        return self.send_json({
            "ok": True,
            "stage": snap["stage"],
            "pct": round(snap["pct"], 4),
            "ready": snap["stage"] == "done",
            "url": snap["url"],
            "error": snap["error"],
            "from": snap["from"],
            "to": snap["to"],
            "bytes": snap["bytes"],
            "encoder": snap["encoder"],
        })

    # ---------- /api/upload + /api/localfile (local videos) ----------

    def handle_upload(self, qs):
        """Receive a local video file and store it so it can be played, seeked
        and cut just like any URL. Returns a URL under /api/localfile/."""
        name = (qs.get("name") or ["video.mp4"])[0]
        ext = os.path.splitext(name)[1].lower() or ".mp4"
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length <= 0:
            return self.send_json({"ok": False, "error": "empty upload"}, 400)

        fid = uuid.uuid4().hex
        try:
            os.makedirs(_UPLOAD_DIR, exist_ok=True)
            fpath = os.path.join(_UPLOAD_DIR, fid + ext)
            remaining = length
            with open(fpath, "wb") as f:
                while remaining > 0:
                    chunk = self.rfile.read(min(CHUNK_SIZE, remaining))
                    if not chunk:
                        break
                    f.write(chunk)
                    remaining -= len(chunk)
        except OSError as e:
            return self.send_json({"ok": False, "error": "could not save upload: %s" % e}, 500)

        with _uploads_lock:
            _uploads[fid] = fpath
        # Keep the real extension in the URL so it's recognized as direct media
        # by the compiler; the display name is returned separately for the title.
        url = "/api/localfile/%s/file%s" % (fid, ext)
        return self.send_json({"ok": True, "url": url, "name": name})

    def handle_localfile(self, path):
        """Serve an uploaded local video with HTTP Range support (so the browser
        can seek and ffmpeg can fast-seek into it)."""
        parts = path.split("/")  # ['', 'api', 'localfile', '<id>', '<name>']
        fid = parts[3] if len(parts) > 3 else ""
        with _uploads_lock:
            fpath = _uploads.get(fid)
        if not fpath or not os.path.isfile(fpath):
            fpath = _recover_upload(fid)
        if not fpath:
            return self.send_error(404, "not found")

        size = os.path.getsize(fpath)
        ext = os.path.splitext(fpath)[1].lower()
        ctype = MIME.get(ext) or {
            ".mp4": "video/mp4", ".webm": "video/webm", ".mkv": "video/x-matroska",
            ".mov": "video/quicktime", ".m4v": "video/x-m4v", ".ogv": "video/ogg",
        }.get(ext, "application/octet-stream")

        rng = self.headers.get("Range")
        start, end = 0, size - 1
        partial = False
        if rng:
            m = re.match(r"bytes=(\d*)-(\d*)", rng.strip())
            if m:
                if m.group(1):
                    start = int(m.group(1))
                if m.group(2):
                    end = int(m.group(2))
                start = max(0, start)
                end = min(end, size - 1)
                if start > end:
                    start, end = 0, size - 1
                partial = True

        clen = end - start + 1
        self.send_response(206 if partial else 200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(clen))
        self.send_header("Accept-Ranges", "bytes")
        if partial:
            self.send_header("Content-Range", "bytes %d-%d/%d" % (start, end, size))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if self.command == "HEAD":
            return
        try:
            with open(fpath, "rb") as f:
                f.seek(start)
                remaining = clen
                while remaining > 0:
                    chunk = f.read(min(CHUNK_SIZE, remaining))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    remaining -= len(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass

    # ---------- /api/proxy ----------

    def _reresolve_and_open(self, org, qmax, client_range):
        """Stream URL went stale (403/410/…): re-extract from the original
        page and open the fresh stream. Returns (upstream, target, headers,
        impersonated) or None when re-extraction can't save the request."""
        try:
            with _resolve_lock:
                _resolve_cache.pop((org, qmax), None)
            r = resolve_stream(org, qmax)
            hdrs = {k: v for k, v in r["headers"].items() if v}
            hdrs.setdefault("User-Agent", DEFAULT_UA)
            hdrs.setdefault("Accept-Encoding", "identity")
            if client_range:
                hdrs["Range"] = client_range
            imp = r.get("impersonated", False)
            upstream = open_upstream(r["stream_url"], hdrs, imp)
            return upstream, r["stream_url"], r["headers"], imp
        except Exception:  # noqa: BLE001
            return None

    def _open_with_query(self, target, query, req_headers, impersonated):
        """Retry `target` carrying its parent playlist's query string."""
        parts = urllib.parse.urlsplit(target)
        if parts.query:
            return None
        signed = urllib.parse.urlunsplit(
            (parts.scheme, parts.netloc, parts.path, query, parts.fragment))
        try:
            return (open_upstream(signed, req_headers, impersonated),
                    signed, req_headers, impersonated)
        except Exception:  # noqa: BLE001
            return None

    def _open_without_query(self, target, req_headers, impersonated):
        """Retry `target` with its query string removed."""
        parts = urllib.parse.urlsplit(target)
        if not parts.query:
            return None
        bare = urllib.parse.urlunsplit(
            (parts.scheme, parts.netloc, parts.path, "", parts.fragment))
        try:
            return (open_upstream(bare, req_headers, impersonated),
                    bare, req_headers, impersonated)
        except Exception:  # noqa: BLE001
            return None

    def send_proxy_error(self, code, target, detail=""):
        """Fail a proxied request with a reason the UI and console can show.

        The browser turns any failed media fetch into the same opaque error, so
        without this the tile could only offer boilerplate about CORS/DRM. The
        reason is echoed in a CORS-exposed header the front-end reads back.
        """
        host = urllib.parse.urlsplit(target).netloc or target[:60]
        reason = "upstream %s from %s%s" % (code, host,
                                            (" (%s)" % detail) if detail else "")
        sys.stderr.write("  PROXY FAIL %s\n" % reason)
        body = reason.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Expose-Headers", "X-MS-Reason")
        self.send_header("X-MS-Reason", reason)
        self.end_headers()
        try:
            self.wfile.write(body)
        except OSError:
            self.close_connection = True

    def _resign_and_open(self, target, org, qmax, req_headers, impersonated):
        """An HLS sub-resource expired: re-extract the page for a fresh
        signature and move it onto this URL. Returns the same tuple shape as
        `_reresolve_and_open`, or None when it can't be revived."""
        try:
            with _resolve_lock:
                _resolve_cache.pop((org, qmax), None)
            fresh = resolve_stream(org, qmax)
            signed = retoken(target, fresh["stream_url"])
            if not signed:
                return None
            hdrs = {k: v for k, v in fresh["headers"].items() if v}
            hdrs.setdefault("User-Agent", DEFAULT_UA)
            hdrs.setdefault("Accept-Encoding", "identity")
            if req_headers.get("Range"):
                hdrs["Range"] = req_headers["Range"]
            imp = fresh.get("impersonated", impersonated)
            return open_upstream(signed, hdrs, imp), signed, fresh["headers"], imp
        except Exception:  # noqa: BLE001
            return None

    # ---------- /api/diagnose ----------

    def handle_diagnose(self, qs):
        """Walks the whole chain for one link and reports where it breaks.

        A failing tile only ever told us "it didn't play", because the browser
        collapses every media failure into one opaque error and the top-level
        playlist usually fetches fine even when the segments underneath do not.
        This follows page -> stream -> playlist -> segment with the real
        headers and prints the exact status at each hop.

        Query VALUES are never printed: on signed CDNs they are credentials.
        """
        url = (qs.get("url") or [""])[0].strip()
        if not url:
            return self.send_text("usage: /api/diagnose?url=<page url>\n", 400)
        try:
            qmax = int((qs.get("q") or ["480"])[0])
        except ValueError:
            qmax = 480

        out = []
        add = out.append
        add("MultiScreen diagnosis")
        add("page      : %s" % url)
        add("-" * 62)

        # 1. extraction
        t0 = time.time()
        with _resolve_lock:
            _resolve_cache.pop((url, qmax), None)
        try:
            r = resolve_stream(url, qmax)
        except Exception as e:  # noqa: BLE001
            add("1 resolve  : FAILED after %.1fs" % (time.time() - t0))
            add("  reason   : %s" % str(e)[:300])
            add("-" * 62)
            add("VERDICT: extraction never produced a stream URL. Nothing was")
            add("         played because there was nothing to play.")
            return self.send_text("\n".join(out) + "\n")

        stream, headers = r["stream_url"], dict(r["headers"])
        imp = r.get("impersonated", False)
        add("1 resolve  : OK (%.1fs)  isHls=%s  impersonated=%s"
            % (time.time() - t0, r["isHls"], imp))
        add("  title    : %s" % (r["title"] or "")[:70])
        add("  stream   : %s" % _safe_url(stream))
        add("  query    : %s" % (_query_keys(stream) or "(none)"))
        add("  headers  : %s" % ", ".join(
            ("Cookie(%d chars)" % len(v)) if k.lower() == "cookie" else k
            for k, v in headers.items()))

        # 2. the stream URL itself
        st, ctype, body, err = _probe(stream, headers, imp)
        add("2 stream   : %s%s" % (_status(st, err),
                                   ("  %s  %d bytes" % (ctype, len(body))) if body else ""))
        if st != 200:
            add("-" * 62)
            add("VERDICT: the stream URL itself is refused. The link resolved")
            add("         but the CDN will not serve it to us.")
            return self.send_text("\n".join(out) + "\n")

        text = body.decode("utf-8", "replace")
        if not text.lstrip().startswith("#EXTM3U"):
            add("-" * 62)
            add("VERDICT: plain media file and it downloads fine. If the tile")
            add("         still fails the problem is in the browser, not here.")
            return self.send_text("\n".join(out) + "\n")

        # 3/4. follow the playlist down to a real segment
        level, parent, parent_headers = 3, stream, headers
        for _ in range(2):
            uris = [l.strip() for l in text.splitlines()
                    if l.strip() and not l.strip().startswith("#")]
            # What the children ARE decides the wording, not how deep we are:
            # a master lists variant playlists, a media playlist lists segments.
            points_at_playlists = bool(uris) and ".m3u8" in uris[0].split("?")[0]
            kind = "variant" if points_at_playlists else "segment"
            add("  kind     : %s playlist, %d URI(s)"
                % ("master" if points_at_playlists else "media", len(uris)))
            if not uris:
                add("-" * 62)
                add("VERDICT: the playlist has no URIs at all - the CDN likely")
                add("         served a block/redirect page instead of a manifest.")
                add("  first bytes: %r" % text[:160])
                return self.send_text("\n".join(out) + "\n")

            child = urllib.parse.urljoin(parent, uris[0])
            pq = urllib.parse.urlsplit(parent).query
            attempts = _attempt_variants(child, pq)
            add("%d %-9s: %s" % (level, kind, _safe_url(child)))
            winner = None
            for label, candidate in attempts:
                st, ctype, cbody, err = _probe(candidate, parent_headers, imp)
                add("  %-14s -> %s" % (label, _status(st, err)))
                if st == 200 and cbody:
                    winner = (candidate, ctype, cbody)
                    break
            if not winner:
                add("-" * 62)
                add("VERDICT: the playlist loads but its %ss are all refused."
                    % kind)
                add("         That is why the tile shows an error even though")
                add("         the stream URL itself fetches fine.")
                return self.send_text("\n".join(out) + "\n")

            cand, ctype, cbody = winner
            if cbody.lstrip().startswith(b"#EXTM3U"):
                text, parent, level = cbody.decode("utf-8", "replace"), cand, level + 1
                continue
            add("-" * 62)
            add("VERDICT: full chain works (page -> playlist -> segment).")
            add("         Playback should succeed; if the tile still fails the")
            add("         problem is in the browser/player, not the network.")
            return self.send_text("\n".join(out) + "\n")

        add("-" * 62)
        add("VERDICT: playlists nest deeper than expected; stopped here.")
        return self.send_text("\n".join(out) + "\n")

    def send_text(self, text, status=200):
        data = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def handle_proxy(self, qs):
        token = (qs.get("p") or [""])[0]
        if not token:
            return self.send_error(400, "missing token")
        try:
            tok = decode_target(token)
            target, headers = tok["url"], tok.get("headers", {})
            impersonated = bool(tok.get("imp"))
        except Exception:  # noqa: BLE001
            return self.send_error(400, "bad token")

        req_headers = {k: v for k, v in headers.items() if v}
        req_headers.setdefault("User-Agent", DEFAULT_UA)
        # A compressed body would break the Content-Length passthrough below.
        req_headers.setdefault("Accept-Encoding", "identity")
        # Forward Range so <video> can seek.
        client_range = self.headers.get("Range")
        if client_range:
            req_headers["Range"] = client_range

        # Whole-body request we've served before (HLS segments on a later loop,
        # or a second tile playing the same video): answer straight from RAM.
        # Keyed on the token's URL, which stays fixed even when healing swaps
        # the URL we actually fetch — otherwise every healed segment would
        # store itself under a key no later request ever looks up.
        cache_key = target
        if not client_range:
            cached = media_cache_get(cache_key)
            if cached is not None:
                return self.send_cached(*cached)

        # Stream URLs go stale (expiry, rolling 403 enforcement on googlevideo
        # & friends) and CDNs occasionally just drop a request. When we know
        # the original page, one fresh extraction usually revives the tile
        # without the browser ever noticing.
        try:
            upstream = open_upstream(target, req_headers, impersonated)
        except Exception as e:  # noqa: BLE001
            code = e.code if isinstance(e, urllib.error.HTTPError) else 502
            healed = None
            refused = code in (401, 403, 404, 410)
            # Cheapest fix first: a segment refused because RFC-3986 joining
            # stripped its parent playlist's signature. Just put it back — no
            # network round-trip, no re-extraction.
            if refused and tok.get("pq"):
                healed = self._open_with_query(target, tok["pq"], req_headers,
                                               impersonated)
            # Inverse case: we signed the segment up front and this CDN signs
            # the full URL, so our extra query broke it. Try it bare.
            if healed is None and refused and tok.get("sub"):
                healed = self._open_without_query(target, req_headers,
                                                  impersonated)
            if healed is None and tok.get("org") and (code == 502 or refused):
                if tok.get("sub"):
                    healed = self._resign_and_open(
                        target, tok["org"], tok.get("q"), req_headers, impersonated)
                else:
                    healed = self._reresolve_and_open(
                        tok["org"], tok.get("q"), client_range)
            if healed is None:
                return self.send_proxy_error(code, target)
            upstream, target, headers, impersonated = healed
            req_headers = {k: v for k, v in headers.items() if v}
            req_headers.setdefault("User-Agent", DEFAULT_UA)
            req_headers.setdefault("Accept-Encoding", "identity")
            if client_range:
                req_headers["Range"] = client_range

        ctype = upstream.header("Content-Type", "application/octet-stream")
        looks_hls = "mpegurl" in ctype.lower() or is_hls(target)

        if looks_hls:
            # Rewrite the manifest so segments/keys also go through the proxy.
            body = upstream.read()
            upstream.close()
            try:
                text = body.decode("utf-8", "replace")
                rewritten = self.rewrite_hls(
                    text, target, headers, impersonated,
                    org=tok.get("org"), qmax=tok.get("q")).encode("utf-8")
            except Exception:  # noqa: BLE001
                rewritten = body
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.apple.mpegurl")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(rewritten)))
            self.end_headers()
            self.wfile.write(rewritten)
            return

        # Binary content (mp4/segments): stream it through.
        self.send_response(upstream.status)
        clen = None
        for h in ("Content-Type", "Content-Length", "Content-Range"):
            v = upstream.header(h)
            if v:
                if h == "Content-Length":
                    clen = v
                self.send_header(h, v)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Expose-Headers", "Content-Length, Content-Range")
        if clen is None:
            # Without Content-Length an HTTP/1.1 keep-alive response has no
            # body framing: the browser would wait forever for "the rest" and
            # the tile looks frozen. Close the connection to mark the end.
            self.send_header("Connection", "close")
            self.close_connection = True
        # Small whole-body responses are worth keeping for the next lap.
        collect = None
        if (not client_range and upstream.status == 200 and clen and
                clen.isdigit() and int(clen) <= MEDIA_CACHE_MAX_ITEM):
            collect = []

        self.end_headers()
        ok = self.pump_body(upstream, target, req_headers, impersonated,
                            clen, client_range, collect)
        if ok and collect is not None:
            media_cache_put(cache_key, ctype, b"".join(collect))

    def send_cached(self, ctype, body):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Expose-Headers", "Content-Length, Content-Range")
        self.end_headers()
        try:
            self.wfile.write(body)
        except OSError:
            self.close_connection = True

    def pump_body(self, upstream, target, req_headers, impersonated,
                  clen, client_range, collect=None):
        """Copies the upstream body to the browser, resuming if it breaks.

        A CDN dropping the connection halfway used to mean the browser got a
        body shorter than the Content-Length we already promised: the tile
        froze or errored out, and the wall lost a video for good. We know
        exactly how many bytes made it out, so we just re-open the stream with
        a Range starting there and carry on — the player never notices.

        Returns True when the whole body was delivered (the caller may then
        cache what `collect` gathered).
        """
        expected = int(clen) if clen and clen.isdigit() else None
        origin = 0
        if client_range:
            m = re.match(r"bytes=(\d+)", client_range.strip())
            if m:
                origin = int(m.group(1))

        sent, attempts = 0, 0
        while True:
            try:
                for chunk in upstream.chunks():
                    try:
                        self.wfile.write(chunk)
                    except OSError:
                        # Client closed the player mid-stream; entirely normal.
                        upstream.close()
                        self.close_connection = True
                        return False
                    sent += len(chunk)
                    if collect is not None:
                        collect.append(chunk)
                upstream.close()
                return expected is None or sent >= expected
            except Exception:  # noqa: BLE001 — upstream died, not the client
                upstream.close()
            if expected is None or sent >= expected or attempts >= 2:
                # Nothing left to resume onto (or we've tried enough): drop the
                # connection so the browser sees a truncated body immediately
                # instead of waiting out the promised Content-Length.
                self.close_connection = True
                return False
            attempts += 1
            resume = dict(req_headers)
            resume["Range"] = "bytes=%d-" % (origin + sent)
            try:
                upstream = open_upstream(target, resume, impersonated)
            except Exception:  # noqa: BLE001
                self.close_connection = True
                return False
            if upstream.status != 206:
                # The host ignored our Range and restarted from byte 0 —
                # appending that would corrupt what the player already has.
                upstream.close()
                self.close_connection = True
                return False

    def rewrite_hls(self, text, base_url, headers, impersonated=False,
                    org=None, qmax=None):
        """Rewrites HLS manifest URIs to go through /api/proxy.

        Each URI gets an absolute URL on a round-robin shard port, so segment
        downloads for many players spread across connections instead of
        queuing behind the browser's per-host limit. The originating page is
        carried into every child URI so expired segments can be re-signed
        instead of killing the tile.
        """
        # Relative URIs in a playlist resolve per RFC 3986, which DROPS the
        # parent's query string. On token-signed CDNs (phncdn & co.) that
        # signature is the only thing making the segment fetchable, so every
        # segment came back 403 and the tile died before playing a frame.
        # We keep the parent's query alongside the URL and retry with it if
        # the plain fetch is refused — never blindly, since some CDNs sign the
        # full URL and an extra query would itself break the signature.
        parent_query = urllib.parse.urlsplit(base_url).query

        # When the parent's query carries a signature, apply it up front rather
        # than discovering the need via a 403. Probing unsigned first meant one
        # rejected request per segment; across a 40-tile wall that flood of
        # 403s is itself what makes a CDN start rate-limiting us.
        signed_parent = _looks_signed(parent_query)

        def proxy_for(u):
            absolute = urllib.parse.urljoin(base_url, u)
            if not urllib.parse.urlsplit(absolute).query and parent_query:
                if signed_parent:
                    # Sign now; `pq=""` records that the bare URL is the fallback.
                    absolute = absolute + "?" + parent_query
                    return self.proxy_url(absolute, headers, impersonated,
                                          org=org, qmax=qmax, sub=True)
                return self.proxy_url(absolute, headers, impersonated,
                                      org=org, qmax=qmax, sub=True,
                                      pq=parent_query)
            return self.proxy_url(absolute, headers, impersonated,
                                  org=org, qmax=qmax, sub=True)

        out = []
        attr_uri = re.compile(r'URI="([^"]+)"')
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                out.append(line)
                continue
            if stripped.startswith("#"):
                # Rewrites URI="..." in EXT-X-KEY / EXT-X-MEDIA / EXT-X-MAP tags.
                if "URI=" in stripped:
                    line = attr_uri.sub(lambda m: 'URI="%s"' % proxy_for(m.group(1)), line)
                out.append(line)
            else:
                out.append(proxy_for(stripped))
        return "\n".join(out)

    # ---------- /api/voices, /api/tts ----------

    def handle_voices(self, qs):
        lang = (qs.get("lang") or [""])[0]
        gender = (qs.get("gender") or [""])[0].lower()
        try:
            voices = tts_voices_for(lang, gender)
        except ImportError:
            return self.send_json({"ok": False, "reason": "missing",
                                   "error": "Neural voices need edge-tts: "
                                            "pip install edge-tts"}, 503)
        except Exception as e:
            return self.send_json({"ok": False, "reason": "offline",
                                   "error": str(e)[:200]}, 502)
        return self.send_json({"ok": True, "voices": voices})

    def handle_tts(self, qs):
        text = (qs.get("text") or [""])[0].strip()[:TTS_MAX_CHARS]
        voice = (qs.get("voice") or [""])[0].strip()
        rate = _tts_pct((qs.get("rate") or ["+0%"])[0])
        pitch = _tts_hz((qs.get("pitch") or ["+0Hz"])[0])
        if not text:
            return self.send_json({"ok": False, "error": "no text"}, 400)
        if not re.match(r"^[a-zA-Z]{2,3}(-[A-Za-z0-9]{2,20}){1,3}Neural$", voice):
            return self.send_json({"ok": False, "error": "bad voice name"}, 400)
        try:
            data = tts_render(text, voice, rate, pitch)
        except ImportError:
            return self.send_json({"ok": False, "reason": "missing",
                                   "error": "Neural voices need edge-tts: "
                                            "pip install edge-tts"}, 503)
        except Exception as e:
            return self.send_json({"ok": False, "reason": "offline",
                                   "error": str(e)[:200]}, 502)
        self.send_response(200)
        self.send_header("Content-Type", "audio/mpeg")
        self.send_header("Access-Control-Allow-Origin", "*")
        # Same phrase, same voice, same bytes — let the browser keep it too.
        self.send_header("Cache-Control", "public, max-age=86400")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # ---------- helpers ----------

    # ---------- /api/cast (mirror the wall to the paired TV) ----------

    def _cast_body(self):
        # Always called, even by handlers that ignore the body: on a kept-alive
        # connection an unread body becomes the start of the next request.
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length > 0 else b""
        try:
            return json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:  # noqa: BLE001
            return {}

    def handle_cast_devices(self, qs):
        """Every renderer on the LAN, each marked with how the lock sees it."""
        paired = _cast_load_device()
        devices = cast_discover()
        for d in devices:
            d["paired"] = bool(paired and d["uuid"] == paired["uuid"])
            d["verdict"] = _cast_check(paired, d) if paired else None
        return self.send_json({"ok": True, "paired": paired, "devices": devices})

    def handle_cast_pair(self):
        data = self._cast_body()
        try:
            rec = cast_pair(str(data.get("uuid") or "").strip())
        except CastRefused as exc:
            return self.send_json({"ok": False, "error": str(exc)}, 403)
        return self.send_json({"ok": True, "paired": rec})

    def handle_cast_unpair(self):
        self._cast_body()
        with _cast_lock:
            busy = _cast["state"] in ("starting", "casting")
        if busy:
            return self.send_json({"ok": False, "error": "stop the cast first."}, 409)
        _cast_clear_device()
        return self.send_json({"ok": True, "paired": None})

    def handle_cast_start(self):
        data = self._cast_body()
        source = "test" if str(data.get("source") or "") == "test" else "screen"
        try:
            return self.send_json(cast_start(source))
        except CastRefused as exc:
            return self.send_json(dict(cast_status(), ok=False, error=str(exc)), 403)
        except Exception as exc:  # noqa: BLE001
            return self.send_json(dict(cast_status(), ok=False,
                                       error="cast failed: %s" % exc), 500)

    def handle_cast_stop(self):
        self._cast_body()
        return self.send_json(cast_stop())

    def handle_cast_status(self, qs):
        return self.send_json(cast_status())

    def send_json(self, obj, status=200):
        data = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class Server(ThreadingHTTPServer):
    # Video chunks go out in small bursts; Nagle's algorithm can hold each
    # write for up to ~200 ms waiting for a full packet. Send immediately.
    disable_nagle_algorithm = True
    request_queue_size = 64

    # On Windows SO_REUSEADDR does NOT mean "reuse a port stuck in TIME_WAIT"
    # the way it does on Unix — it lets a SECOND process bind a port that is
    # already serving, and Windows then hands incoming connections to one or
    # the other at random. Two MultiScreens (the one you just started and an
    # older one still open behind it) each answer part of the traffic, so the
    # app appears to run old and new code at the same time: some tiles work,
    # some fail, and edits look like they did nothing. Refuse to share.
    allow_reuse_address = (os.name != "nt")

    def server_bind(self):
        if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.socket.setsockopt(socket.SOL_SOCKET,
                                   socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


def main():
    global PROXY_PORTS, _port_cycle
    flags = {a for a in sys.argv[1:] if a.startswith("--")}
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    base = int(args[0]) if args else 8000
    if "--cast-selftest" in flags:
        sys.exit(cast_selftest())
    _cast_reap_orphan()

    servers = []
    ports = []
    for p in [base] + [base + i + 1 for i in range(EXTRA_PROXY_PORTS)]:
        try:
            servers.append(Server(("127.0.0.1", p), Handler))
            ports.append(p)
        except OSError:
            if p == base:
                # An older instance is almost certainly still running. Say so
                # plainly: a bare traceback here looks like a crash, the stale
                # server keeps answering the browser, and every change made to
                # this file appears to have done nothing.
                print("")
                print("  Port %d is already in use." % p)
                print("  Another MultiScreen is still running - this one did NOT start,")
                print("  and the browser is talking to the OLD code.")
                print("")
                print("  Close the other window (or run:  taskkill /F /IM python.exe )")
                print("  and start this one again.")
                print("")
                sys.exit(1)
            print("  (port %d busy, skipping this shard)" % p)

    PROXY_PORTS = ports
    _port_cycle = itertools.cycle(ports)

    print("MultiScreen running at  http://localhost:%d" % base)
    try:
        import yt_dlp
        print("yt-dlp version:         %s  (keep it fresh: pip install -U yt-dlp)"
              % yt_dlp.version.__version__)
    except ImportError:
        print("yt-dlp NOT installed:   pip install -U yt-dlp  (needed for generic sites)")
    if _IMPERSONATE is not None:
        print("Browser impersonation:  ON (%s)  — fallback for 403/410 blocks" % _IMPERSONATE)
    else:
        print("Browser impersonation:  OFF — install it to beat 403/410 blocks: "
              "pip install -U curl_cffi")
    if FFMPEG:
        print("Compilation (ffmpeg):   ON  — encoder: %s%s" % (
            VIDEO_ENCODER,
            "  (GPU-accelerated)" if VIDEO_ENCODER != "libx264" else ""))
    else:
        print("Compilation (ffmpeg):   OFF — enable it with: pip install imageio-ffmpeg")
    if len(ports) > 1:
        print("Proxy shards on ports:  %s  (spreads browser connections)"
              % ", ".join(str(p) for p in ports[1:]))
    # Doubles as a build marker: if this line is missing from the console, an
    # older server.py is running and none of the current fixes are live.
    print("Segment RAM cache:      %d MB  — looping tiles stop re-hitting the CDN"
          % (MEDIA_CACHE_MAX_BYTES // (1024 * 1024)))
    _paired = _cast_load_device()
    if _paired:
        print("Cast to TV:             paired with %s (%s) - the only screen it will ever use"
              % (_paired["name"], _paired["model"]))
    else:
        print("Cast to TV:             not paired - open Cast in the app to pair your TV")
    print("Ctrl+C to stop.")

    for httpd in servers[1:]:
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        servers[0].serve_forever()
    except KeyboardInterrupt:
        print("\nStopping…")
        cast_stop()
        for httpd in servers:
            httpd.shutdown()


if __name__ == "__main__":
    main()
