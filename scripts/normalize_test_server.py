from __future__ import annotations

import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.crawl_url_normalizer import canonicalize_crawl_url  # noqa: E402


HTML = """<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Crawl URL Normalize Test</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, sans-serif; margin: 32px; max-width: 980px; }
    textarea, input { width: 100%; box-sizing: border-box; font: 14px/1.4 ui-monospace, SFMono-Regular, Consolas, monospace; }
    textarea { min-height: 180px; padding: 12px; }
    button { margin-top: 12px; padding: 9px 14px; cursor: pointer; }
    table { width: 100%; border-collapse: collapse; margin-top: 24px; table-layout: fixed; }
    th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; word-break: break-all; }
    th { background: #f6f6f6; text-align: left; }
    .muted { color: #666; }
  </style>
</head>
<body>
  <h1>Crawl URL Normalize Test</h1>
  <p class="muted">한 줄에 URL 하나씩 넣고 정규화 결과를 확인하세요.</p>
  <textarea id="urls">HTTPS://WWW.Ex.Go.Kr:443/Board/List.do/?pageIndex=2&utm_source=x&B=2&a=1#hash
http://www.ex.go.kr:80/Board/View.do?utm_campaign=x&page=3&NttId=10&BbsId=A#top
ex.go.kr/foo/?z=2&y=1&pageNo=9</textarea>
  <button id="run">Normalize</button>
  <table>
    <thead><tr><th>raw</th><th>normalized</th></tr></thead>
    <tbody id="rows"></tbody>
  </table>
  <script>
    async function normalize() {
      const urls = document.getElementById("urls").value.split(/\\r?\\n/).map(s => s.trim()).filter(Boolean);
      const res = await fetch("/api/normalize", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ urls })
      });
      const data = await res.json();
      document.getElementById("rows").innerHTML = (data.results || []).map(item =>
        `<tr><td>${escapeHtml(item.raw)}</td><td>${escapeHtml(item.normalized)}</td></tr>`
      ).join("");
    }
    function escapeHtml(value) {
      return String(value || "").replace(/[&<>"']/g, ch => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
      }[ch]));
    }
    document.getElementById("run").addEventListener("click", normalize);
    normalize();
  </script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("cache-control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/normalize":
            qs = parse_qs(parsed.query)
            urls = qs.get("url") or qs.get("urls") or []
            payload = _normalize_payload(urls)
            self._send(200, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
            return
        if parsed.path in {"", "/"}:
            self._send(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
            return
        self._send(404, b"not found", "text/plain; charset=utf-8")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/api/normalize":
            self._send(404, b"not found", "text/plain; charset=utf-8")
            return
        try:
            length = int(self.headers.get("content-length") or "0")
            raw = self.rfile.read(length).decode("utf-8") if length else "{}"
            body = json.loads(raw)
        except Exception:
            body = {}
        urls = body.get("urls") if isinstance(body, dict) else []
        if isinstance(urls, str):
            urls = [urls]
        payload = _normalize_payload(urls if isinstance(urls, list) else [])
        self._send(200, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[normalize-test] {self.address_string()} {fmt % args}", flush=True)


def _normalize_payload(urls: list[object]) -> dict[str, object]:
    results = []
    for value in urls:
        raw = str(value or "").strip()
        if not raw:
            continue
        try:
            normalized = canonicalize_crawl_url(raw)
            error = ""
        except Exception as exc:
            normalized = ""
            error = str(exc)
        item = {"raw": raw, "normalized": normalized}
        if error:
            item["error"] = error
        results.append(item)
    return {"ok": True, "count": len(results), "results": results}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"normalize test server: http://{args.host}:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
