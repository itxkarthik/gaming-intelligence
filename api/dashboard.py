"""Phase 5 — Dashboard: server-rendered views (Jinja2) + Datastar SSE.

Every view renders COMPLETE HTML server-side (works with JS disabled);
Datastar (one script tag, no build step) only merges `datastar-patch-elements`
events pushed over SSE from this module. One long-lived stream per view,
opened by `data-init="@get('/sse/<view>')"` in the page.

Data sources are shared process state (state.py): the Kafka EventFeed fan-out
for the live stream, the offset sampler for KPIs, the shared async Redis
readers for live entities, the engine alert log (alerts:stream) and
PostgreSQL for alert history. Fragment renderers are pure functions of the
data they are given; the same renderer feeds the initial page and SSE.
"""

import asyncio
import html
import json
import logging
import os
import time
from typing import Any
from urllib.parse import urlencode

from datastar_py import ServerSentEventGenerator as SSE
from datastar_py.consts import ElementPatchMode as PatchMode
from datastar_py.fastapi import DatastarResponse
from fastapi import APIRouter, HTTPException, Request
from fastapi.templating import Jinja2Templates

import state
from state import as_float

log = logging.getLogger("api.dashboard")

router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates"))

NAV = [
    ("overview", "Overview", "/"),
    ("servers", "Servers", "/servers"),
    ("anticheat", "Anti-Cheat", "/anticheat"),
    ("matches", "Match Quality", "/matches"),
    ("behavior", "Player Behavior", "/behavior"),
    ("tournament", "Tournament", "/tournament"),
    ("alerts", "Alerts", "/alerts"),
]

# Publisher cadences (seconds). Templates print the same numbers.
POLL = {
    "rows": 1,              # overview live-feed batch
    "kpi": 5,               # overview KPI / alert / flagged panels
    "servers": 2,
    "anticheat": 2,
    "matches": 2,
    "tournament": 3,
    "behavior": 3,
    "alerts_keepalive": 15,
}
LIVE_ROWS = 12
ALERTS_PAGE_SIZE = 20

templates.env.globals.update(
    VERSION=state.VERSION,
    POLL=POLL,
    ALERT_TYPES=state.ALERT_TYPES,
    ALERT_SEVERITIES=state.ALERT_SEVERITIES,
    SUSPICION_FLAG=state.SUSPICION_FLAG,
    SUSPICION_CRITICAL=state.SUSPICION_CRITICAL,
    MATCH_BALANCED_MIN=state.MATCH_BALANCED_MIN,
    MATCH_UNBALANCED_MIN=state.MATCH_UNBALANCED_MIN,
)

E = html.escape


def _page(request: Request, name: str, active: str, sse: str | None = None,
          status_code: int = 200, **ctx: Any):
    context = {"nav": NAV, "active": active, "sse": sse, **ctx}
    return templates.TemplateResponse(request=request, name=name, context=context,
                                      status_code=status_code)


def _not_found(request: Request, active: str, what: str, ident: str):
    return _page(request, "notfound.html", active, status_code=404, what=what, ident=ident)


def unavailable_page(request: Request, service: str):
    """503 page for Redis/PostgreSQL outages (main.py exception handler)."""
    return _page(request, "unavailable.html", "", status_code=503, service=service)


def _patch(html_fragment: str, selector: str):
    return SSE.patch_elements(html_fragment, selector=selector, mode=PatchMode.INNER)


def _suspicion_class(eff: float) -> str:
    return ("bad" if eff >= state.SUSPICION_CRITICAL
            else "warn" if eff >= state.SUSPICION_FLAG else "ok")


def _match_class(status: str) -> str:
    return "bad" if status == "STOMPED" else "warn" if status == "UNBALANCED" else "ok"


# ─── Fragment renderers (used for BOTH initial page render and SSE patches) ─

ALERT_LABEL = {
    "CHEAT_DETECTED": "Cheat",
    "SMURF_DETECTED": "Smurf",
    "BEHAVIOR_ANOMALY": "Behaviour shift",
    "SERVER_DEGRADED": "Server degraded",
    "MATCH_QUALITY_LOW": "Low-quality match",
}
SEVERITY_CLASS = {"CRITICAL": "bad", "WARNING": "warn"}


def _alerts_last_minute() -> int:
    cutoff_ms = (time.time() - 60) * 1000
    return sum(1 for a in state.alert_log.recent(state.ALERT_LOG_SIZE)
               if as_float(a.get("timestamp")) >= cutoff_ms)


async def _kpi_dict() -> dict[str, Any]:
    snap = state.throughput_snapshot()
    counts = await state.current_counts()
    return {
        "eps": snap["events_per_sec"],
        "chart_eps": snap["latest_events_per_sec"],
        "chart_time": snap["latest_sample_time"],
        "total_events": snap["ingested_total"],
        "live_matches": state.feed.live_matches(),
        "alerts_per_min": _alerts_last_minute(),
        **counts,
    }


def _kpi(label: str, value: str, unit: str = "") -> str:
    unit_html = f'<span class="kpi-unit">{E(unit)}</span>' if unit else ""
    return (f'<div class="kpi"><div class="kpi-label">{E(label)}</div>'
            f'<div class="kpi-value">{E(value)}{unit_html}</div></div>')


def kpis_html(k: dict[str, Any]) -> str:
    eps = f"{k['eps']:,.0f}" if k["eps"] is not None else "—"
    # The alert log holds the last ALERT_LOG_SIZE alerts, so a full log
    # within the minute is a floor, not a count.
    capped = k["alerts_per_min"] >= state.ALERT_LOG_SIZE
    alerts_rate = f"{k['alerts_per_min']}{'+' if capped else ''}"
    card_html = "".join([
        _kpi("Throughput", eps, "ev/s"),
        _kpi("Ingested", f"{k['total_events']:,}"),
        _kpi("Live matches", str(k["live_matches"])),
        _kpi("Flagged cheaters", str(k["cheat_flagged"])),
        _kpi("Smurf accounts", str(k["smurf"])),
        _kpi("Behaviour shifts", str(k["behavior_anomalies"])),
        _kpi("Alerts", alerts_rate, "/ min"),
    ])
    # Chart carrier: the latest 5 s sample (same series as the initial
    # timeline); the page dedupes on data-time so a sample plots once.
    chart_eps = "" if k["chart_eps"] is None else k["chart_eps"]
    carrier = (f'<div id="telemetry-carrier" data-eps="{chart_eps}" '
               f'data-time="{E(k["chart_time"] or "")}" '
               f'data-total="{k["total_events"]}" style="display:none"></div>')
    return card_html + carrier


def alerts_html(n: int = 10) -> str:
    """Last n Go-engine alerts (alerts:stream, seeded from alert_history)."""
    rows = state.alert_log.recent(n)
    if not rows:
        return '<li class="empty">No alerts yet — the engine forwards within seconds of a detection.</li>'
    out = []
    for a in rows:
        ts = as_float(a.get("timestamp")) / 1000
        when = state.clock(ts) if ts else "—"
        severity = str(a.get("severity", "INFO"))
        kind = str(a.get("alert_type", "?"))
        msg = E(str(a.get("message", "")))
        out.append(
            f'<li class="alert">'
            f'<span class="sev {SEVERITY_CLASS.get(severity, "")}" title="{E(severity)}"></span>'
            f'<span class="alert-entity">{E(str(a.get("entity_id") or "—"))}</span>'
            f'<span class="alert-type">{E(ALERT_LABEL.get(kind, kind))}</span>'
            f'<time>{when}</time>'
            f'<span class="alert-msg" title="{msg}">{msg}</span></li>')
    return "".join(out)


def flagged_html(rows: list[dict[str, str]], n: int = 10) -> str:
    if not rows:
        return '<li class="empty">No players flagged.</li>'
    out = []
    for p in rows[:n]:
        pid = E(str(p.get("player_id", "?")))
        eff = state.effective_suspicion(p)
        signals = "".join(
            f'<span class="chip kind" title="{title}">{name}</span>'
            for name, title, on in (
                ("IF", "Isolation Forest outlier", p.get("iforest_flag") == "true"),
                ("CUSUM", "Sustained accuracy shift", p.get("behavior_anomaly") == "true"),
            ) if on)
        out.append(f'<li><a class="mono" href="/players/{pid}">{pid}</a>'
                   f'<span class="signals">{signals}</span>'
                   f'<span class="score {_suspicion_class(eff)}">{eff * 100:.0f}%</span></li>')
    return "".join(out)


def _worst_first(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return sorted(rows, key=lambda s: as_float(s.get("health_score"), 101.0))


def server_rows_html(rows: list[dict[str, str]]) -> str:
    if not rows:
        return '<tr class="empty"><td colspan="8">No server metrics yet.</td></tr>'
    out = []
    for s in _worst_first(rows):
        hs = as_float(s.get("health_score"))
        status = str(s.get("status", "?"))
        cls = state.SERVER_STATUS_CLASS.get(status, "warn")
        out.append(
            f'<tr>'
            f'<td class="mono">{E(str(s.get("server_id", "?")))}</td>'
            f'<td>{E(str(s.get("region", "")))}</td>'
            f'<td class="bar-cell"><div class="bar"><div class="bar-fill {cls}" '
            f'style="width:{max(2, min(100, hs)):.0f}%"></div></div>'
            f'<span class="bar-num {cls}">{hs:.1f}</span></td>'
            f'<td><span class="chip {cls}">{E(status)}</span></td>'
            f'<td class="num">{as_float(s.get("avg_cpu")):.0f}%</td>'
            f'<td class="num">{as_float(s.get("avg_ram")):.0f}%</td>'
            f'<td class="num">{as_float(s.get("avg_latency")):.0f} ms</td>'
            f'<td class="num">{E(str(s.get("peak_players", "0")))}</td>'
            f'</tr>')
    return "".join(out)


def servers_stats_html(rows: list[dict[str, str]]) -> str:
    scores = [as_float(s.get("health_score")) for s in rows]
    avg = sum(scores) / len(scores) if scores else 0.0
    degraded = sum(1 for s in rows if s.get("status") != state.SERVER_HEALTHY)
    return (
        f'<span class="stat"><b>{len(rows)}</b> servers</span>'
        f'<span class="stat"><b>{avg:.1f}</b> avg health</span>'
        f'<span class="stat {"warn" if degraded else ""}"><b>{degraded}</b> degraded</span>'
    )


def cheat_rows_html(rows: list[dict[str, str]]) -> str:
    if not rows:
        return '<tr class="empty"><td colspan="7">No flagged players — detections appear within seconds of a suspicious match.</td></tr>'
    out = []
    for p in rows:
        pid = E(str(p.get("player_id", "?")))
        base = as_float(p.get("suspicion_score"))
        eff = state.effective_suspicion(p)
        boost = max(0.0, eff - base)
        cls = _suspicion_class(eff)
        ml_flag = '<span class="chip warn">IF</span>' if p.get("iforest_flag") == "true" else "—"
        out.append(
            f'<tr>'
            f'<td><a class="mono" href="/players/{pid}">{pid}</a></td>'
            f'<td class="bar-cell"><div class="bar"><div class="bar-fill {cls}" '
            f'style="width:{max(2, min(100, eff * 100)):.0f}%"></div></div>'
            f'<span class="bar-num {cls}">{eff:.2f}</span></td>'
            f'<td class="num">{base:.2f}</td>'
            f'<td class="num {"warn" if boost > 0 else ""}">+{boost:.2f}</td>'
            f'<td class="num">{E(p.get("iforest_score") or "—")}</td>'
            f'<td>{ml_flag}</td>'
            f'<td class="num">{E(str(p.get("total_shots", "0")))}</td>'
            f'</tr>')
    return "".join(out)


def match_rows_html(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr class="empty"><td colspan="7">No matches scored yet.</td></tr>'
    out = []
    for m in rows:
        mid = E(str(m.get("match_id", "?")))
        score = m["quality_score"]
        status = str(m.get("status", "?"))
        cls = _match_class(status)
        out.append(
            f'<tr>'
            f'<td><a class="mono" href="/matches/{mid}">{mid}</a></td>'
            f'<td class="bar-cell"><div class="bar"><div class="bar-fill {cls}" '
            f'style="width:{max(2, min(100, score)):.0f}%"></div></div>'
            f'<span class="bar-num {cls}">{score:.1f}</span></td>'
            f'<td><span class="chip {cls}">{E(status)}</span></td>'
            f'<td class="num">{E(str(m.get("total_kills", "0")))}</td>'
            f'<td class="num">{as_float(m.get("kill_imbalance")):.2f}</td>'
            f'<td class="num">{E(str(m.get("disconnects", "0")))}</td>'
            f'<td class="num">{E(str(m.get("duration_s", "0")))}s</td>'
            f'</tr>')
    return "".join(out)


BEHAVIOR_ROWS = 100  # most significant rows per table; the page states the total


def _top_note(shown: int, total: int) -> str:
    return f"top {shown} of {total}" if total > shown else f"{total}"


def behavior_rows_html(rows: list[dict[str, str]]) -> str:
    rows = sorted(rows, key=lambda a: -abs(as_float(a.get("last_z"))))[:BEHAVIOR_ROWS]
    if not rows:
        return ('<tr class="empty"><td colspan="5">No behaviour shifts yet — '
                'a player needs a 10-step baseline before shifts are judged.</td></tr>')
    out = []
    for a in rows:
        pid = E(str(a.get("player_id", "?")))
        out.append(
            f'<tr>'
            f'<td><a class="mono" href="/players/{pid}">{pid}</a></td>'
            f'<td class="num">{E(str(a.get("last_batch_mean", "—")))}</td>'
            f'<td class="num">{E(str(a.get("baseline_mean", "—")))}'
            f' <span class="hint">n={E(str(a.get("baseline_n", "0")))}</span></td>'
            f'<td class="num warn">{E(str(a.get("last_z", "—")))}</td>'
            f'<td class="num">{E(str(a.get("anomalies", "0")))}</td>'
            f'</tr>')
    return "".join(out)


def smurf_rows_html(rows: list[dict[str, str]]) -> str:
    rows = rows[:BEHAVIOR_ROWS]  # read_smurfs() is most-likely first
    if not rows:
        return '<tr class="empty"><td colspan="5">No smurfs flagged yet (needs scored combat).</td></tr>'
    out = []
    for s in rows:
        pid = E(str(s.get("player_id", "?")))
        prob = as_float(s.get("probability"))
        out.append(
            f'<tr>'
            f'<td><a class="mono" href="/players/{pid}">{pid}</a></td>'
            f'<td class="bar-cell"><div class="bar"><div class="bar-fill warn" '
            f'style="width:{max(2, min(100, prob * 100)):.0f}%"></div></div>'
            f'<span class="bar-num warn">{prob:.2f}</span></td>'
            f'<td class="num">{E(str(s.get("account_age_days", "—")))}d</td>'
            f'<td class="num">{E(str(s.get("games_played", "—")))}</td>'
            f'<td class="mono">{E(str(s.get("archetype", "—")))}</td>'
            f'</tr>')
    return "".join(out)


def histogram_html(buckets: list[int]) -> str:
    total = sum(buckets)
    carrier = (f'<div id="hist-carrier" data-buckets="{E(json.dumps(buckets))}" '
               f'data-total="{total}" style="display:none"></div>')
    summary = (f'<span class="hint" style="font-family:var(--mono)">Total Scored: '
               f'<b style="color:var(--text)">{total}</b> matches</span>')
    return carrier + summary


def matches_stats_html(summary: dict[str, Any], recent: int) -> str:
    return (
        f'<span class="stat"><b>{summary["tracked"]}</b> tracked</span>'
        f'<span class="stat"><b>{recent}</b> scored last {state.MATCH_RECENT_SECONDS}s</span>'
        f'<span class="stat ok"><b>{summary["balanced"]}</b> balanced</span>'
        f'<span class="stat warn"><b>{summary["unbalanced"]}</b> unbalanced</span>'
        f'<span class="stat bad"><b>{summary["stomped"]}</b> stomped</span>'
    )


def tournament_html(snap: dict[str, Any]) -> str:
    srv, mt, pl, al = snap["servers"], snap["matches"], snap["players"], snap["alerts"]
    worst = mt["worst"]
    if worst:
        wid = E(str(worst["match_id"]))
        worst_html = (f'Worst match: <a class="mono" href="/matches/{wid}">{wid}</a>'
                      f' · score {worst["score"]:.1f}')
    else:
        worst_html = "No matches scored yet."
    avg = srv["avg_health"] if srv["avg_health"] is not None else "—"
    cards = [
        ("Servers", f'{srv["active"]} active', f'avg health {avg} · {srv["degraded"]} degraded'),
        ("Matches", f'{mt["scored_total"]} scored', worst_html),
        ("Players", f'{pl["cheat_flagged"]} flagged',
         f'{pl["smurf"]} smurf · {pl["behavior_anomalies"]} behavior shifts'),
        ("Alerts", f'{al["recent_count"]} recent',
         'full history in the <a href="/alerts">alert feed</a>'),
    ]
    return "".join(
        f'<div class="kpi"><div class="kpi-label">{E(label)}</div>'
        f'<div class="kpi-value">{E(value)}</div>'
        f'<div>{sub}</div></div>'
        for label, value, sub in cards)


def _event_clock(env: dict[str, Any], ev: dict[str, Any]) -> str:
    """Event time (event_time / server-metric timestamp, else Kafka record
    time) — not the time the row was rendered."""
    ms = as_float(ev.get("event_time") or ev.get("timestamp") or env.get("timestamp"))
    return state.clock(ms / 1000) if ms > 0 else "—"


def _fmt_event(item: str) -> str | None:
    """Kafka envelope JSON → one live-stream <tr>; None = not displayable.
    Parts are built raw and escaped exactly once on output."""
    try:
        env = json.loads(item)
    except (ValueError, TypeError):
        return None
    ev = env.get("event") if isinstance(env, dict) else None
    if not isinstance(ev, dict):
        return None
    topic = env.get("topic")
    kind = str(ev.get("event_type") or "?")

    if topic == "gameplay_events":
        # One SHOT per trigger pull. Misses are far too noisy for humans;
        # hits render as damage, lethal hits as kills.
        if not ev.get("hit"):
            return None
        desc = f'{ev.get("player_id") or "?"} → {ev.get("target_player_id") or "?"}'
        wep = str(ev.get("weapon_id") or "")
        headshot = " · HS" if ev.get("is_headshot") else ""
        place = str(ev.get("match_id") or "")
        if ev.get("is_kill"):
            kind, cls = "KILL", "k-kill"
            assist = ev.get("assister_id")
            detail = wep + headshot + (f" · assist {assist}" if assist else "")
        else:
            kind, cls = "DAMAGE", "k-dmg"
            detail = f"{wep} · {int(as_float(ev.get('damage')))} dmg{headshot}"
    elif topic == "player_events":
        desc = str(ev.get("player_id") or "?")
        md = ev.get("metadata")
        md = md if isinstance(md, dict) else {}
        detail = " · ".join(f"{k}={str(v)[:24]}" for k, v in list(md.items())[:4])
        place = str(ev.get("match_id") or "")
        cls = "k-player"
    elif topic == "server_metrics":
        kind, cls = "SERVER", "k-server"
        desc = str(ev.get("server_id") or "?")
        detail = (f"cpu {as_float(ev.get('cpu_percent')):.0f}% · "
                  f"lat {as_float(ev.get('avg_latency_ms')):.0f}ms · "
                  f"{int(as_float(ev.get('active_players')))}p")
        place = str(ev.get("region") or "")
    else:
        return None

    return (f'<tr class="{cls}"><td class="mono dim">{E(_event_clock(env, ev))}</td>'
            f'<td><span class="chip kind {cls}">{E(kind)}</span></td>'
            f'<td class="evt-desc">{E(desc)}</td>'
            f'<td class="dim">{E(detail)}</td>'
            f'<td class="mono dim">{E(place)}</td></tr>')


def _latest_rows(raw_events: list[str], n: int = LIVE_ROWS) -> list[str]:
    """Newest-first displayable rows from oldest-first raw envelopes."""
    rows = []
    for raw in reversed(raw_events):
        row = _fmt_event(raw)
        if row:
            rows.append(row)
            if len(rows) >= n:
                break
    return rows


def initial_event_rows_html() -> str:
    recent = _latest_rows(state.feed.recent_events())
    if recent:
        return "".join(recent)
    return ('<tr class="empty"><td colspan="5">'
            'Waiting for streaming events — start the simulator (<code>make simulator-run</code>)…'
            '</td></tr>')


# ─── Pages ──────────────────────────────────────────────────────────────────
# Redis/PostgreSQL errors propagate to main.py's handler (503 page).

@router.get("/")
async def view_overview(request: Request):
    return _page(request, "overview.html", "overview", sse="/sse/overview",
                 kpis=kpis_html(await _kpi_dict()),
                 alerts=alerts_html(6),
                 flagged=flagged_html(await state.read_flagged_players()),
                 event_rows=initial_event_rows_html(),
                 initial_timeline=state.throughput_timeline())


@router.get("/servers")
async def view_servers(request: Request):
    rows = await state.read_servers()
    return _page(request, "servers.html", "servers", sse="/sse/servers",
                 stats=servers_stats_html(rows), rows=server_rows_html(rows))


@router.get("/anticheat")
async def view_anticheat(request: Request):
    return _page(request, "anticheat.html", "anticheat", sse="/sse/anticheat",
                 rows=cheat_rows_html(await state.read_flagged_players()))


@router.get("/matches")
async def view_matches(request: Request):
    recent = await state.read_recent_matches()
    summary = await state.read_match_summary()
    return _page(request, "matches.html", "matches", sse="/sse/matches",
                 stats=matches_stats_html(summary, recent["recent"]),
                 rows=match_rows_html(recent["rows"]),
                 histogram=histogram_html(summary["histogram"]),
                 initial_buckets=summary["histogram"])


@router.get("/matches/{match_id}")
async def view_match_detail(request: Request, match_id: str):
    data = await state.read_match(match_id)
    if not data:
        return _not_found(request, "matches", "match", match_id)
    return _page(request, "match_detail.html", "matches",
                 m=data, match_id=match_id, score=data["quality_score"])


@router.get("/players/{player_id}")
async def view_player_detail(request: Request, player_id: str):
    p = await state.read_player(player_id)
    if p is None:
        return _not_found(request, "anticheat", "player", player_id)
    profile = p["combat"] or {}
    base = as_float(profile.get("suspicion_score"))
    return _page(request, "player_detail.html", "anticheat",
                 player_id=player_id, profile=profile,
                 smurf=p["smurf_evaluation"] or {}, behavior=p["behavior"] or {},
                 cusum=p["cusum"] or {}, flags=p["flags"],
                 base=base, eff=state.effective_suspicion(profile))


@router.get("/behavior")
async def view_behavior(request: Request):
    anomalies = await state.read_behavior_anomalies()
    smurfs = await state.read_smurfs()
    return _page(request, "behavior.html", "behavior", sse="/sse/behavior",
                 behavior_rows=behavior_rows_html(anomalies),
                 smurf_rows=smurf_rows_html(smurfs),
                 behavior_count=_top_note(min(len(anomalies), BEHAVIOR_ROWS), len(anomalies)),
                 smurf_count=_top_note(min(len(smurfs), BEHAVIOR_ROWS), len(smurfs)))


@router.get("/tournament")
async def view_tournament(request: Request):
    return _page(request, "tournament.html", "tournament", sse="/sse/tournament",
                 cards=tournament_html(await state.tournament_snapshot()))


_KEEP = object()   # qs() sentinel: keep the current value (None = clear it)


@router.get("/alerts")
async def view_alerts(request: Request, alert_type: str | None = None,
                      severity: str | None = None, page: int = 0):
    alert_type = alert_type or None
    severity = severity or None
    page = max(0, page)
    hist = {"alerts": [], "total": 0}
    hist_error = False
    try:
        hist = await state.fetch_alert_history(ALERTS_PAGE_SIZE, page * ALERTS_PAGE_SIZE,
                                               alert_type, severity)
        last = max(0, (hist["total"] + ALERTS_PAGE_SIZE - 1) // ALERTS_PAGE_SIZE - 1)
        if page > last:                       # clamp past-the-end pages
            page = last
            hist = await state.fetch_alert_history(ALERTS_PAGE_SIZE, page * ALERTS_PAGE_SIZE,
                                                   alert_type, severity)
    except state.UNAVAILABLE_ERRORS as e:
        log.warning("alert history unavailable: %r", e)
        hist_error = True
    max_page = max(0, (hist["total"] + ALERTS_PAGE_SIZE - 1) // ALERTS_PAGE_SIZE - 1)
    cur_type, cur_sev, cur_page = alert_type, severity, page

    def qs(alert_type: Any = _KEEP, severity: Any = _KEEP, page: Any = _KEEP) -> str:
        params = {
            "alert_type": cur_type if alert_type is _KEEP else alert_type,
            "severity": cur_sev if severity is _KEEP else severity,
            "page": cur_page if page is _KEEP else page,
        }
        params = {k: v for k, v in params.items() if v not in (None, "", 0)}
        return "/alerts" + ("?" + urlencode(params) if params else "")

    return _page(request, "alerts.html", "alerts", sse="/sse/alerts",
                 status_code=503 if hist_error else 200,
                 live=alerts_html(10), history=hist, hist_error=hist_error,
                 alert_type=alert_type, severity=severity, page=page,
                 max_page=max_page, qs=qs)


# ─── SSE streams (one shared publisher per view) ────────────────────────────

def _sse(bus: state.Broadcaster) -> DatastarResponse:
    """Subscriber side of a shared broadcast: paint cached frames instantly,
    then yield every frame the single publisher produces from now on."""
    async def gen():
        q = bus.subscribe()
        try:
            for frame in bus.frames():
                yield frame
            while True:
                yield await q.get()
        finally:
            bus.unsubscribe(q)

    return DatastarResponse(gen())


async def _pub_overview(bus: state.Broadcaster):
    """ONE task for all windows: batches feed rows every 1 s and rebuilds the
    KPI/alerts/flagged panels every 5 s — each frame computed once, then the
    identical bytes go to every subscriber. A Redis outage only pauses the
    Redis-backed panels; the Kafka rows keep streaming."""
    q = state.feed.register()
    streak = state.FailureStreak("overview KPI panels")
    recent = _latest_rows(state.feed.recent_events())
    if recent:
        bus.publish("rows", _patch("".join(recent), "#event-rows"))
    tick = 0
    loop = asyncio.get_running_loop()
    try:
        while True:
            batch: list[str] = []
            deadline = loop.time() + POLL["rows"]
            while (left := deadline - loop.time()) > 0:
                try:
                    item = await asyncio.wait_for(q.get(), timeout=left)
                except TimeoutError:
                    break
                row = _fmt_event(item)
                if row:
                    batch.append(row)
            if batch:
                recent = (batch[::-1] + recent)[:LIVE_ROWS]
                bus.publish("rows", _patch("".join(recent), "#event-rows"))
            tick += 1
            if tick % (POLL["kpi"] // POLL["rows"]) == 0:
                bus.publish("alerts", _patch(alerts_html(6), "#recent-alerts"))
                try:
                    kpis = kpis_html(await _kpi_dict())
                    flagged = flagged_html(await state.read_flagged_players())
                except state.UNAVAILABLE_ERRORS as e:
                    streak.fail(e)
                    continue
                streak.ok()
                bus.publish("kpi", _patch(kpis, "#kpis"))
                bus.publish("flagged", _patch(flagged, "#flagged-mini"))
    finally:
        state.feed.unregister(q)


async def _pub_servers(bus: state.Broadcaster):
    while True:
        rows = await state.read_servers()
        bus.publish("rows", _patch(server_rows_html(rows), "#server-rows"))
        bus.publish("stats", _patch(servers_stats_html(rows), "#servers-stats"))
        await asyncio.sleep(POLL["servers"])


async def _pub_anticheat(bus: state.Broadcaster):
    while True:
        rows = await state.read_flagged_players()
        bus.publish("rows", _patch(cheat_rows_html(rows), "#cheat-rows"))
        await asyncio.sleep(POLL["anticheat"])


async def _pub_matches(bus: state.Broadcaster):
    while True:
        recent = await state.read_recent_matches()
        summary = await state.read_match_summary()
        bus.publish("rows", _patch(match_rows_html(recent["rows"]), "#match-rows"))
        bus.publish("hist", _patch(histogram_html(summary["histogram"]), "#histogram"))
        bus.publish("stats", _patch(matches_stats_html(summary, recent["recent"]),
                                    "#matches-stats"))
        await asyncio.sleep(POLL["matches"])


async def _pub_tournament(bus: state.Broadcaster):
    while True:
        snap = await state.tournament_snapshot()
        bus.publish("cards", _patch(tournament_html(snap), "#tournament-cards"))
        await asyncio.sleep(POLL["tournament"])


async def _pub_behavior(bus: state.Broadcaster):
    while True:
        anomalies = await state.read_behavior_anomalies()
        smurfs = await state.read_smurfs()
        bus.publish("rows", _patch(behavior_rows_html(anomalies), "#behavior-rows"))
        bus.publish("smurfs", _patch(smurf_rows_html(smurfs), "#smurf-rows"))
        counts = (f'<span id="behavior-counts" class="hint">'
                  f'{_top_note(min(len(anomalies), BEHAVIOR_ROWS), len(anomalies))} shifts · '
                  f'{_top_note(min(len(smurfs), BEHAVIOR_ROWS), len(smurfs))} smurfs</span>')
        bus.publish("counts", _patch(counts, "#behavior-counts"))
        await asyncio.sleep(POLL["behavior"])


async def _pub_alerts(bus: state.Broadcaster):
    """Re-render the live alert list whenever the engine publishes (via the
    process-wide alert listener), plus a keepalive re-patch when idle."""
    q = state.alert_bus.subscribe()
    try:
        while True:
            bus.publish("rows", _patch(alerts_html(10), "#alert-rows"))
            try:
                await asyncio.wait_for(q.get(), timeout=POLL["alerts_keepalive"])
            except TimeoutError:
                continue
            while not q.empty():              # coalesce bursts into one frame
                q.get_nowait()
    finally:
        state.alert_bus.unsubscribe(q)


BUSES = {name: state.Broadcaster(name, pub) for name, pub in (
    ("overview", _pub_overview),
    ("servers", _pub_servers),
    ("anticheat", _pub_anticheat),
    ("matches", _pub_matches),
    ("tournament", _pub_tournament),
    ("behavior", _pub_behavior),
    ("alerts", _pub_alerts),
)}


@router.get("/sse/{view}")
async def sse_view(view: str):
    bus = BUSES.get(view)
    if bus is None:
        raise HTTPException(status_code=404, detail=f"no stream {view!r}")
    return _sse(bus)
