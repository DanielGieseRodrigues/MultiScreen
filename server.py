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

  GET /api/proxy?p=<base64>
      Pipes the video/manifest through the server, injecting the headers
      (Referer/User-Agent) the site requires and enabling CORS for the
      browser. For HLS playlists (.m3u8), internal URIs are rewritten to
      also go through the proxy — so hls.js can play them.

Performance: browsers only open ~6 simultaneous connections per host:port.
With 12 videos all streaming through one port, half of them starve. So the
server also listens on a few extra ports and spreads proxied streams across
them round-robin.

Usage:
    python server.py            # port 8000 (+ proxy shards on 8001-8005)
    python server.py 8080       # custom port
"""

import base64
import collections
import itertools
import json
import os
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


def resolve_stream(url, qmax):
    """Resolve a page/stream URL to a direct muxed (audio+video) stream.

    Returns a dict {title, stream_url, headers, isHls}. Results are cached (the
    same cache used by /api/resolve). Raises ImportError if yt-dlp is missing,
    or ResolveError with a helpful message on extraction failure.
    """
    cache_key = (url, qmax)
    now = time.time()
    with _resolve_lock:
        hit = _resolve_cache.get(cache_key)
        if hit and hit[0] > now:
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

    def build(enc):
        cmd = [FFMPEG, "-y", "-hide_banner", "-loglevel", "error"]
        if seek is not None:
            cmd += ["-ss", "%.3f" % seek]
        cmd += _ffmpeg_input_opts(source, headers)
        cmd += ["-i", source]
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

    Direct media (mp4/m3u8/local) → ffmpeg fast-seek (downloads only the span).
    Everything else (YouTube/Twitch/…) → yt-dlp downloads the span, then we
    normalize the local file. Returns (index, path_or_None, error_message).
    """
    url = cut["url"]
    start = cut["start"]
    dur = cut["end"] - cut["start"]
    out = os.path.join(tmpdir, "seg_%03d.ts" % index)

    if _is_direct_media(url):
        ok, err = _normalize_clip(url, {}, start, dur, out, resolution, encoder)
        if ok:
            return index, out, ""
        # A local path that failed can't be recovered; a hotlink-protected http
        # file might still work through yt-dlp, so fall through in that case.
        if not url.lower().startswith(("http://", "https://")):
            return index, None, err

    # Resolve + download just the span with yt-dlp (handles 403/DASH/merging).
    try:
        src = _download_section(url, start, cut["end"], qmax, tmpdir, index)
    except Exception as e:  # noqa: BLE001
        msg = (str(e).splitlines() or ["download failed"])[-1]
        return index, None, msg[:200]
    if not src:
        return index, None, "yt-dlp produced no clip file"

    ok, err = _normalize_clip(src, {}, None, None, out, resolution, encoder)
    try:
        os.remove(src)
    except OSError:
        pass
    return (index, out, "") if ok else (index, None, err)


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
        if path == "/api/compile/result":
            return self.handle_compile_result(qs)
        if path.startswith("/api/localfile/"):
            return self.handle_localfile(path)
        return self.handle_static(path)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/api/compile":
            return self.handle_compile()
        if parsed.path == "/api/upload":
            return self.handle_upload(qs)
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

    # ---------- helpers ----------

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
    base = int(sys.argv[1]) if len(sys.argv) > 1 else 8000

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
    print("Ctrl+C to stop.")

    for httpd in servers[1:]:
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        servers[0].serve_forever()
    except KeyboardInterrupt:
        print("\nStopping…")
        for httpd in servers:
            httpd.shutdown()


if __name__ == "__main__":
    main()
