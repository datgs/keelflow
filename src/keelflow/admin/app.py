"""Minimal server-rendered SuperAdmin (deliberately rough).

Serves: / (metrics+queues), /jobs, /jobs/<id>, /runs, /runs/<id>,
/jobs/<id>/rows, POST /jobs/<id>/retry. No auth — bind locally or put
behind your own gateway.
"""
from __future__ import annotations

import asyncio
import html
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import psycopg

from ..engine.monitoring import MonitoringService


def make_app(dsn: str, host: str = "127.0.0.1", port: int = 8410):
    """Start the admin HTTP server (blocking). Ctrl+C to stop."""
    svc = MonitoringService()

    def q(coro):
        async def _run():
            conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
            try:
                return await coro(conn)
            finally:
                await conn.close()
        return asyncio.run(_run())

    class H(BaseHTTPRequestHandler):
        def _send(self, body: str, code: int = 200):
            data = body.encode()
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):  # quieter
            pass

        def do_GET(self):
            p = urlparse(self.path)
            path, qs = p.path, parse_qs(p.query)
            if path == "/":
                return self._page("keelflow admin", self._home())
            if path == "/jobs":
                return self._page("jobs", self._jobs(qs))
            if path.startswith("/jobs/") and path.endswith("/rows"):
                jid = path.split("/")[2]
                return self._page("failed rows", self._rows(jid))
            if path.startswith("/jobs/"):
                return self._page("job", self._job(path.split("/")[2]))
            if path == "/runs":
                return self._page("runs", self._runs())
            if path.startswith("/runs/"):
                return self._page("run", self._run(path.split("/")[2]))
            return self._send("<h1>404</h1>", 404)

        def do_POST(self):
            p = urlparse(self.path)
            if p.path.startswith("/jobs/") and p.path.endswith("/retry"):
                jid = p.path.split("/")[2]
                q(lambda c: svc.retry_job(c, jid))
                return self._send("<meta http-equiv='refresh' content='0;/jobs'>")
            return self._send("<h1>404</h1>", 404)

        # ── views ────────────────────────────────────────────────────

        def _page(self, title, body):
            nav = ("<p><a href='/'>home</a> · <a href='/jobs'>jobs</a> · "
                   "<a href='/runs'>runs</a></p>")
            return self._send(
                f"<html><head><title>{title}</title>"
                "<style>body{{font-family:monospace;margin:2em}}"
                "table{{border-collapse:collapse}}td,th{{border:1px solid #ccc;"
                "padding:4px 8px}} .failed{{color:#b00}} .pending{{color:#a60}}"
                " .done{{color:#070}}</style></head><body>"
                f"{nav}<h1>{title}</h1>{body}</body></html>"
            )

        def _esc(self, v):
            return html.escape(str(v)) if v is not None else ""

        def _table(self, rows, cols, links=None):
            links = links or {}
            out = ["<table><tr>" + "".join(f"<th>{c}</th>" for c in cols) + "</tr>"]
            for r in rows:
                cells = []
                for c in cols:
                    v = r.get(c)
                    cls = v if c == "status" and v in ("failed", "pending", "done") else ""
                    cell = f"<td class='{cls}'>{self._esc(v)}</td>"
                    if c in links:
                        cell = f"<td><a href='{links[c]}{r[c]}'>{self._esc(v)}</a></td>"
                    cells.append(cell)
                out.append("<tr>" + "".join(cells) + "</tr>")
            out.append("</table>")
            return "".join(out)

        def _home(self):
            m = q(svc.metrics_snapshot)
            return (
                f"<h2>metrics</h2><ul>"
                f"<li>queue depth: {m['queue_depth']}</li>"
                f"<li>running: {m['running']}</li>"
                f"<li>failed (24h): {m['failed_24h']}</li>"
                f"<li>oldest pending: {self._esc(round(m['oldest_pending_s'] or 0,1))}s</li>"
                f"</ul>"
            )

        def _jobs(self, qs):
            types = qs.get("type", [None])[0]
            statuses = qs.get("status", [None])[0]
            res = q(lambda c: svc.list_jobs(
                c, types=[types] if types else None,
                statuses=[statuses] if statuses else None, limit=100,
            ))
            rows = [
                {
                    "id": str(r["id"]), "type": r["type"], "status": r["status"],
                    "attempts": r["attempts"], "run_at": r["run_at"],
                    "claimed_by": r["claimed_by"], "assignee": r["assignee"],
                    "created_at": r["created_at"],
                }
                for r in res["items"]
            ]
            return self._table(
                rows, ["id", "type", "status", "attempts", "assignee", "created_at"],
                links={"id": "/jobs/"},
            )

        def _job(self, jid):
            r = q(lambda c: svc.get_job(c, jid))
            if not r:
                return "<p>not found</p>"
            kv = "".join(
                f"<tr><td>{k}</td><td>{self._esc(v)}</td></tr>"
                for k, v in r.items() if k != "id"
            )
            return (
                f"<table><tr><td>id</td><td>{r['id']}</td></tr>{kv}</table>"
                f"<p><a href='/jobs/{r['id']}/rows'>failed rows</a> · "
                f"<form method='post' action='/jobs/{r['id']}/retry'>"
                "<button>retry</button></form></p>"
            )

        def _runs(self):
            rows = q(svc.list_runs)
            return self._table(
                [{**r, "id": str(r["id"])} for r in rows],
                ["id", "workflow", "status", "created_at"],
                links={"id": "/runs/"},
            )

        def _run(self, rid):
            r = q(lambda c: svc.get_run(c, rid))
            if not r:
                return "<p>not found</p>"
            steps = [
                {
                    "step_id": s["step_id"], "type": s["type"],
                    "status": s["status"], "attempts": s["attempts"],
                    "outputs": s["outputs"], "job_id": s["job_id"],
                }
                for s in r["steps"]
            ]
            return (
                f"<p>workflow: {r['workflow']} · status: {r['status']}</p>"
                + self._table(steps, ["step_id", "type", "status", "attempts", "outputs"])
            )

        def _rows(self, jid):
            rows = q(lambda c: svc.rows_list_failed(c, jid))
            return self._table(
                [{**r, "id": str(r["id"])} for r in rows],
                ["id", "row_key", "error", "updated_at"],
            )

    server = ThreadingHTTPServer((host, port), H)
    print(f"keelflow admin → http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":  # pragma: no cover
    import os

    make_app(os.environ["KEELFLOW_DSN"])
