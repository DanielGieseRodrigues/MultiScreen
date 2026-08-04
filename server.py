#!/usr/bin/env python3
"""
MultiScreen — local server.

Serves the static app (index.html) and exposes a small API:

  GET /api/resolve?url=<page>
      Uses yt-dlp to find the real stream on ~1800 video sites.
      Returns JSON: { ok, title, stream, isHls } where `stream` is a URL
      that already goes through our /api/proxy (with the right headers).
      Results are cached for a few minutes so reloads are instant.

  GET /api/scan?url=<page>
      Finds every <video> element on the page and returns one proxied entry
      per video (Referer set to the page), so a page with N players becomes
      N tiles. The front-end tries this before the yt-dlp resolve.

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
import itertools
import json
import os
import shutil
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

    _SESSION = _requests.Session()
    _SESSION.trust_env = False  # no surprise proxies from env vars
    for _scheme in ("http://", "https://"):
        _SESSION.mount(_scheme, _HTTPAdapter(pool_connections=16, pool_maxsize=64))
except ImportError:
    _SESSION = None

CHUNK_SIZE = 262144


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
    """Response wrapper: pooled keep-alive connection via requests."""

    def __init__(self, resp):
        self._resp = resp
        self.status = resp.status_code

    def header(self, name, default=None):
        return self._resp.headers.get(name, default)

    def read(self):
        return self._resp.content

    def chunks(self):
        return self._resp.iter_content(CHUNK_SIZE)

    def close(self):
        self._resp.close()


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
    """Response wrapper: curl_cffi with browser impersonation (streamed)."""

    def __init__(self, resp):
        self._resp = resp
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


# curl_cffi sessions are not thread-safe; keep one per handler thread so
# impersonated streams still reuse keep-alive connections.
_curl_local = threading.local()


def _open_upstream_curl(target, headers):
    from curl_cffi import requests as curl_requests
    sess = getattr(_curl_local, "session", None)
    if sess is None:
        sess = curl_requests.Session(impersonate="chrome")
        _curl_local.session = sess
    resp = sess.get(target, headers=headers, stream=True,
                    timeout=30, allow_redirects=True)
    if resp.status_code >= 400:
        code = resp.status_code
        resp.close()
        raise urllib.error.HTTPError(target, code, "upstream error", None, None)
    return _CurlUpstream(resp)


def open_upstream(target, headers, impersonated=False):
    """GETs `target`, reusing pooled keep-alive connections when possible.

    `impersonated` marks streams whose extraction needed browser
    impersonation — their CDNs fingerprint clients too, so fetch them with
    curl_cffi from the start. Plain fetches that bounce with 403 also get one
    impersonated retry (hotlink protection that only bites at download time).
    """
    if impersonated and _IMPERSONATE is not None:
        return _open_upstream_curl(target, headers)
    try:
        if _SESSION is not None:
            resp = _SESSION.get(target, headers=headers, stream=True,
                                timeout=(10, 30), allow_redirects=True)
            if resp.status_code >= 400:
                code = resp.status_code
                resp.close()
                raise urllib.error.HTTPError(target, code, "upstream error", None, None)
            return _RequestsUpstream(resp)
        req = urllib.request.Request(target, headers=headers)
        return _UrllibUpstream(urllib.request.urlopen(req, timeout=30))
    except urllib.error.HTTPError as e:
        if e.code == 403 and _IMPERSONATE is not None:
            return _open_upstream_curl(target, headers)
        raise


def next_proxy_port():
    """Round-robin over the listening ports to spread browser connections."""
    with _port_lock:
        return next(_port_cycle)


def encode_target(url, headers, impersonated=False, org=None, qmax=None):
    """Packs the proxy token: stream URL + headers, plus the impersonation
    flag and — for resolved streams — the ORIGINAL page URL and quality, so
    the proxy can re-extract on the spot when the stream URL goes stale
    (expired link, rolling 403 enforcement)."""
    obj = {"url": url, "headers": headers}
    if impersonated:
        obj["imp"] = True
    if org:
        obj["org"] = org
        obj["q"] = qmax
    raw = json.dumps(obj).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


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
            "best[vcodec!=none][acodec!=none]/best"
        ) % (qmax, qmax, qmax, qmax)
    return "best[vcodec!=none][acodec!=none]/best"


class ResolveError(Exception):
    """yt-dlp couldn't resolve a stream. Carries a user-facing message + status."""

    def __init__(self, message, status=502):
        super().__init__(message)
        self.message = message
        self.status = status


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

    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "format": build_format(qmax),
    }
    # Plain client first; impersonation only as a fallback (see
    # _setup_impersonation for why always-on impersonation breaks YouTube).
    info = None
    used_impersonation = False
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:  # noqa: BLE001
        if _IMPERSONATE is not None:
            try:
                with yt_dlp.YoutubeDL({**opts, "impersonate": _IMPERSONATE}) as ydl:
                    info = ydl.extract_info(url, download=False)
                used_impersonation = True
            except Exception as e2:  # noqa: BLE001
                e = e2
        if info is None:
            msg = str(e).splitlines()[-1] if str(e) else "extraction failed"
            low = msg.lower()
            if "410" in low or "gone" in low or "generic" in low or "unable to download webpage" in low:
                msg += "  →  yt-dlp may be outdated (pip install -U yt-dlp)"
                if _IMPERSONATE is None:
                    msg += " or the site blocks non-browser clients (pip install -U curl_cffi)"
            elif ("403" in low or "forbidden" in low) and _IMPERSONATE is None:
                msg += "  →  site may block non-browser clients: pip install -U curl_cffi"
            raise ResolveError(msg)

    if info is None:
        raise ResolveError("nothing found")
    if "entries" in info:  # playlist -> take the first entry
        entries = [e for e in info["entries"] if e]
        if not entries:
            raise ResolveError("empty playlist")
        info = entries[0]

    stream_url = info.get("url")
    if not stream_url and info.get("requested_formats"):
        stream_url = info["requested_formats"][0].get("url")
    if not stream_url and info.get("formats"):
        for f in reversed(info["formats"]):
            if f.get("url") and f.get("vcodec") != "none" and f.get("acodec") != "none":
                stream_url = f["url"]
                info = {**info, **f}
                break
    if not stream_url:
        raise ResolveError("no playable stream (may require audio/video merging or DRM)")

    headers = dict(info.get("http_headers") or {})
    headers.setdefault("User-Agent", DEFAULT_UA)
    result = {
        "title": info.get("title") or info.get("webpage_url_basename") or url,
        "stream_url": stream_url,
        "headers": headers,
        "isHls": is_hls(stream_url, info.get("protocol", "")),
        "impersonated": used_impersonation,
    }

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
    try:
        if _SESSION is not None:
            r = _SESSION.get(page_url, headers=headers, timeout=(10, 30),
                             allow_redirects=True)
            if r.status_code >= 400:
                raise urllib.error.HTTPError(page_url, r.status_code,
                                             "upstream error", None, None)
            return r.content[:max_bytes].decode("utf-8", "replace"), False
        req = urllib.request.Request(page_url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read(max_bytes).decode("utf-8", "replace"), False
    except Exception as e:  # noqa: BLE001
        if _IMPERSONATE is None:
            raise ResolveError("couldn't fetch the page: %s" % e)
        try:
            from curl_cffi import requests as curl_requests
            r = curl_requests.get(page_url, headers=headers, timeout=30,
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


def scan_page_videos(page_url):
    """Finds every <video> element on a page and returns one media URL each.

    A <video> can carry its stream in a src attribute or in child <source>
    tags; multiple <source> children are quality/format ALTERNATES of the same
    video, so only the first playable one counts. Returns
    (page_title, [absolute media urls], used_impersonation).
    """
    import html as _html

    text, used_imp = _fetch_page_html(page_url)

    tm = _TITLE_RE.search(text)
    page_title = re.sub(r"\s+", " ", tm.group(1)).strip() if tm else ""

    videos, seen = [], set()

    def add(candidate):
        """Records a playable candidate; returns True when it was usable."""
        candidate = (candidate or "").strip()
        if not candidate or candidate.startswith(("blob:", "data:")):
            return False
        absolute = urllib.parse.urljoin(page_url, _html.unescape(candidate))
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

    return page_title, videos, used_imp


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

    outtmpl = os.path.join(tmpdir, "src_%03d.%%(ext)s" % index)
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "noplaylist": True,
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
        if path == "/api/proxy":
            return self.handle_proxy(qs)
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

    def proxy_url(self, target, headers, impersonated=False, org=None, qmax=None):
        """Absolute proxy URL on the next shard port (round-robin)."""
        token = urllib.parse.quote(encode_target(target, headers, impersonated,
                                                 org=org, qmax=qmax))
        return "http://%s:%d/api/proxy?p=%s" % (
            self.request_host(), next_proxy_port(), token)

    def handle_resolve(self, qs):
        url = (qs.get("url") or [""])[0].strip()
        if not url:
            return self.send_json({"ok": False, "error": "Empty URL"}, 400)
        # Quality cap (height in px). Defaults to 480p so many tiles can play
        # at once. q=0 (or negative) = no cap.
        try:
            qmax = int((qs.get("q") or ["480"])[0])
        except ValueError:
            qmax = 480

        try:
            r = resolve_stream(url, qmax)
        except ImportError:
            return self.send_json(
                {"ok": False, "error": "yt-dlp is not installed (pip install yt-dlp)"}, 500)
        except ResolveError as e:
            return self.send_json({"ok": False, "error": e.message}, e.status)

        return self.send_json({
            "ok": True,
            "title": r["title"],
            "stream": self.proxy_url(r["stream_url"], r["headers"],
                                     r.get("impersonated", False),
                                     org=url, qmax=qmax),
            "isHls": r["isHls"],
        })

    # ---------- /api/scan (all <video> elements on a page) ----------

    def handle_scan(self, qs):
        """Scans a page for <video> elements and returns one entry per video,
        already proxied with the right Referer, so each can play in its own
        tile. The front-end uses this before falling back to yt-dlp."""
        url = (qs.get("url") or [""])[0].strip()
        if not url:
            return self.send_json({"ok": False, "error": "Empty URL"}, 400)
        try:
            page_title, vids, used_imp = scan_page_videos(url)
        except ResolveError as e:
            return self.send_json({"ok": False, "error": e.message}, e.status)
        except Exception as e:  # noqa: BLE001
            return self.send_json({"ok": False, "error": str(e)[:200]}, 502)

        # Hotlink protection on these files usually checks the Referer; send
        # the page they were found on.
        media_headers = {"User-Agent": DEFAULT_UA, "Referer": url}
        items = []
        for i, v in enumerate(vids):
            name = v.split("?")[0].rsplit("/", 1)[-1] or ("video %d" % (i + 1))
            items.append({
                "url": self.proxy_url(v, media_headers, used_imp),
                "src": v,
                "isHls": is_hls(v),
                "title": (page_title + " · " if page_title else "") + name,
            })
        return self.send_json({"ok": True, "title": page_title, "videos": items})

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

        try:
            upstream = open_upstream(target, req_headers, impersonated)
        except urllib.error.HTTPError as e:
            # Stream URLs go stale (expiry, rolling 403 enforcement on
            # googlevideo & friends). When we know the original page, one
            # fresh extraction usually revives the tile without the browser
            # ever noticing.
            healed = None
            if tok.get("org") and e.code in (401, 403, 404, 410):
                healed = self._reresolve_and_open(
                    tok["org"], tok.get("q"), client_range)
            if healed is None:
                return self.send_error(e.code, "upstream %s" % e.code)
            upstream, target, headers, impersonated = healed
        except Exception as e:  # noqa: BLE001
            return self.send_error(502, "upstream failed: %s" % e)

        ctype = upstream.header("Content-Type", "application/octet-stream")
        looks_hls = "mpegurl" in ctype.lower() or is_hls(target)

        if looks_hls:
            # Rewrite the manifest so segments/keys also go through the proxy.
            body = upstream.read()
            upstream.close()
            try:
                text = body.decode("utf-8", "replace")
                rewritten = self.rewrite_hls(text, target, headers,
                                             impersonated).encode("utf-8")
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
        self.end_headers()
        try:
            for chunk in upstream.chunks():
                self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass  # client closed the player; normal
        finally:
            upstream.close()

    def rewrite_hls(self, text, base_url, headers, impersonated=False):
        """Rewrites HLS manifest URIs to go through /api/proxy.

        Each URI gets an absolute URL on a round-robin shard port, so segment
        downloads for many players spread across connections instead of
        queuing behind the browser's per-host limit.
        """
        def proxy_for(u):
            absolute = urllib.parse.urljoin(base_url, u)
            return self.proxy_url(absolute, headers, impersonated)

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
                raise  # main port is required
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
