#!/usr/bin/env python3
"""
MultiScreen — servidor local.

Serve o app estático (index.html) e expõe uma pequena API:

  GET /api/resolve?url=<pagina>
      Usa yt-dlp para descobrir o stream real de ~1800 sites de vídeo.
      Retorna JSON: { ok, title, stream, isHls } onde `stream` já é uma
      URL que passa pelo nosso /api/proxy (com os cabeçalhos certos).

  GET /api/proxy?p=<base64>
      Faz o vídeo/manifesto passar pelo servidor, injetando os cabeçalhos
      (Referer/User-Agent) que o site exige e liberando CORS para o browser.
      Para playlists HLS (.m3u8), reescreve as URIs internas para também
      passarem pelo proxy — assim o hls.js consegue tocar.

Uso:
    python server.py            # porta 8000
    python server.py 8080       # porta customizada
"""

import base64
import json
import os
import sys
import re
import urllib.parse
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

# Tipos MIME por extensão para os arquivos estáticos.
MIME = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
}


def encode_target(url, headers):
    """Empacota (url + headers) num token base64 usado pelo /api/proxy."""
    raw = json.dumps({"url": url, "headers": headers}).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_target(token):
    raw = base64.urlsafe_b64decode(token.encode("ascii"))
    obj = json.loads(raw.decode("utf-8"))
    return obj["url"], obj.get("headers", {})


def is_hls(url, protocol=""):
    return "m3u8" in (protocol or "") or ".m3u8" in url.split("?")[0].lower()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        # Log enxuto no console.
        sys.stderr.write("  %s\n" % (fmt % args))

    # ---------- roteamento ----------

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if path == "/api/resolve":
            return self.handle_resolve(qs)
        if path == "/api/proxy":
            return self.handle_proxy(qs)
        return self.handle_static(path)

    # ---------- estático ----------

    def handle_static(self, path):
        if path == "/" or path == "":
            path = "/index.html"
        # Impede path traversal.
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

    def handle_resolve(self, qs):
        url = (qs.get("url") or [""])[0].strip()
        if not url:
            return self.send_json({"ok": False, "error": "URL vazia"}, 400)
        try:
            import yt_dlp
        except ImportError:
            return self.send_json(
                {"ok": False, "error": "yt-dlp não está instalado (pip install yt-dlp)"}, 500)

        opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "noplaylist": True,
            # Prefere um único arquivo já com áudio+vídeo (toca direto no browser).
            "format": "best[vcodec!=none][acodec!=none]/best",
        }
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
        except Exception as e:  # noqa: BLE001 — queremos reportar qualquer falha
            msg = str(e).splitlines()[-1] if str(e) else "falha ao extrair"
            return self.send_json({"ok": False, "error": msg}, 502)

        if info is None:
            return self.send_json({"ok": False, "error": "nada encontrado"}, 502)
        if "entries" in info:  # playlist -> pega o primeiro
            entries = [e for e in info["entries"] if e]
            if not entries:
                return self.send_json({"ok": False, "error": "playlist vazia"}, 502)
            info = entries[0]

        stream_url = info.get("url")
        # Alguns extratores devolvem formatos separados; pega o melhor combinado.
        if not stream_url and info.get("requested_formats"):
            stream_url = info["requested_formats"][0].get("url")
        if not stream_url and info.get("formats"):
            for f in reversed(info["formats"]):
                if f.get("url") and f.get("vcodec") != "none" and f.get("acodec") != "none":
                    stream_url = f["url"]
                    info = {**info, **f}
                    break
        if not stream_url:
            return self.send_json({"ok": False, "error": "sem stream reproduzível (pode exigir merge de áudio/vídeo ou DRM)"}, 502)

        headers = dict(info.get("http_headers") or {})
        headers.setdefault("User-Agent", DEFAULT_UA)
        hls = is_hls(stream_url, info.get("protocol", ""))
        token = encode_target(stream_url, headers)
        proxied = "/api/proxy?p=" + urllib.parse.quote(token)
        return self.send_json({
            "ok": True,
            "title": info.get("title") or info.get("webpage_url_basename") or url,
            "stream": proxied,
            "isHls": hls,
        })

    # ---------- /api/proxy ----------

    def handle_proxy(self, qs):
        token = (qs.get("p") or [""])[0]
        if not token:
            return self.send_error(400, "missing token")
        try:
            target, headers = decode_target(token)
        except Exception:  # noqa: BLE001
            return self.send_error(400, "bad token")

        req_headers = {k: v for k, v in headers.items() if v}
        req_headers.setdefault("User-Agent", DEFAULT_UA)
        # Repassa o Range para permitir seek no <video>.
        client_range = self.headers.get("Range")
        if client_range:
            req_headers["Range"] = client_range

        try:
            req = urllib.request.Request(target, headers=req_headers)
            upstream = urllib.request.urlopen(req, timeout=30)
        except urllib.error.HTTPError as e:
            return self.send_error(e.code, "upstream %s" % e.code)
        except Exception as e:  # noqa: BLE001
            return self.send_error(502, "upstream falhou: %s" % e)

        ctype = upstream.headers.get("Content-Type", "application/octet-stream")
        looks_hls = "mpegurl" in ctype.lower() or is_hls(target)

        if looks_hls:
            # Reescreve o manifesto para que segmentos/chaves passem pelo proxy.
            body = upstream.read()
            try:
                text = body.decode("utf-8", "replace")
                rewritten = self.rewrite_hls(text, target, headers).encode("utf-8")
            except Exception:  # noqa: BLE001
                rewritten = body
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.apple.mpegurl")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(rewritten)))
            self.end_headers()
            self.wfile.write(rewritten)
            return

        # Conteúdo binário (mp4/segmentos): faz streaming byte a byte.
        status = upstream.status if hasattr(upstream, "status") else 200
        self.send_response(status)
        for h in ("Content-Type", "Content-Length", "Content-Range"):
            v = upstream.headers.get(h)
            if v:
                self.send_header(h, v)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        try:
            while True:
                chunk = upstream.read(65536)
                if not chunk:
                    break
                self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass  # cliente fechou o player; normal

    def rewrite_hls(self, text, base_url, headers):
        """Reescreve URIs de um manifesto HLS para passarem pelo /api/proxy."""
        def proxy_for(u):
            absolute = urllib.parse.urljoin(base_url, u)
            return "/api/proxy?p=" + urllib.parse.quote(encode_target(absolute, headers))

        out = []
        attr_uri = re.compile(r'URI="([^"]+)"')
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                out.append(line)
                continue
            if stripped.startswith("#"):
                # Reescreve URI="..." em tags EXT-X-KEY / EXT-X-MEDIA / EXT-X-MAP.
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


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print("MultiScreen rodando em  http://localhost:%d" % port)
    print("Ctrl+C para parar.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nParando…")
        httpd.shutdown()


if __name__ == "__main__":
    main()
