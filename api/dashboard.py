"""Phase 5 — Dashboard: server-rendered views (Jinja2) + Datastar SSE.

Every view renders COMPLETE HTML server-side (works with JS disabled);
Datastar (one script tag, no build step) only merges `datastar-patch-elements`
events pushed over SSE from this module. One long-lived stream per view,
opened by `data-init="@get('/sse/<view>')"` in the page.

Data sources are shared process state (state.py): the Kafka EventFeed fan-out
for the live stream, the offset sampler for KPIs, Redis for live entities,
PostgreSQL for alert history.
"""

import asyncio
import html
import json
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
import redis.asyncio as aioredis

from datastar_py import ServerSentEventGenerator as SSE
from datastar_py.consts import ElementPatchMode as PatchMode
from datastar_py.fastapi import DatastarResponse

import state
from state import _float, get_redis_client

router = APIRouter()
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

E = html.escape


def _page(request: Request, name: str, active: str,
          sse: Optional[str] = None, **ctx: Any):
    context = {"nav": NAV, "active": active, "sse": sse}
    context.update(ctx)
    return templates.TemplateResponse(request=request, name=name, context=context)


# ─── Data readers (sync Redis — tiny keys, run inside the event loop) ───────

def _servers() -> List[Dict[str, str]]:
    r = get_redis_client()
    rows = []
    for s_id in r.smembers("servers:active"):
        data = r.hgetall(f"server:{s_id}")
        if data:
            rows.append(data)
    rows.sort(key=lambda s: _float(s.get("health_score"), 101.0))
    return rows


def _flagged() -> List[Dict[str, str]]:
    r = get_redis_client()
    rows = []
    for p_id in r.smembers("players:flagged"):
        data = r.hgetall(f"player:{p_id}")
        if data:
            rows.append(data)
    rows.sort(key=lambda p: _float(p.get("suspicion_effective"),
                                   _float(p.get("suspicion_score"))), reverse=True)
    return rows


def _matches() -> List[Dict[str, Any]]:
    r = get_redis_client()
    now_ms = int(time.time() * 1000)
    rows = []
    for key in r.scan_iter("match:*"):
        if ":" in key[len("match:"):]:
            continue
        data = r.hgetall(key)
        if not data:
            continue
        updated_ms = int(_float(data.get("updated_at"), 0)) * 1000
        data["active"] = 0 <= (now_ms - updated_ms) <= 90_000
        data["quality_score"] = _float(data.get("quality_score"))
        rows.append(data)
    rows.sort(key=lambda m: str(m.get("updated_at", "")), reverse=True)
    return rows


def _recent_alerts(n: int = 10) -> List[Dict[str, Any]]:
    r = get_redis_client()
    out = []
    for raw in r.lrange("alerts:recent", 0, n - 1):
        try:
            out.append(json.loads(raw))
        except ValueError:
            continue
    return out


def _kpi_dict() -> Dict[str, Any]:
    snap = state.throughput_snapshot()
    r = get_redis_client()
    matches = _matches()
    return {
        "eps": snap["events_per_sec"],
        "totals": snap["per_topic_total"],
        "total_events": sum(snap["per_topic_total"].values()),
        "matches_tracked": len(matches),
        "matches_active": sum(1 for m in matches if m["active"]),
        "flagged": r.scard("players:flagged"),
        "smurf": r.scard("players:smurf"),
        "anomalies": r.scard("behavior:anomalies"),
        "alerts_recent": r.llen("alerts:recent"),
    }


def _histogram_buckets() -> List[int]:
    r = get_redis_client()
    buckets = [0] * 10
    for _, score in r.zrange("matches:quality", 0, -1, withscores=True):
        buckets[min(9, max(0, int(score // 10)))] += 1
    return buckets


# ─── Fragment renderers (used for BOTH initial page render and SSE patches) ─

def kpis_html() -> str:
    k = _kpi_dict()
    eps = f"{k['eps']:.0f}" if k["eps"] is not None else "—"
    cards = [
        ("Events / sec", eps, "kpi-live"),
        ("Total events", f"{k['total_events']:,}", ""),
        ("Matches", f"{k['matches_active']} live / {k['matches_tracked']}", ""),
        ("Cheat flagged", str(k["flagged"]), "kpi-warn" if k["flagged"] else ""),
        ("Smurfs", str(k["smurf"]), "kpi-warn" if k["smurf"] else ""),
        ("Behavior shifts", str(k["anomalies"]), "kpi-warn" if k["anomalies"] else ""),
        ("Alerts (recent)", str(k["alerts_recent"]), ""),
    ]
    return "".join(
        f'<div class="kpi {cls}"><div class="kpi-label">{E(label)}</div>'
        f'<div class="kpi-value">{E(value)}</div></div>'
        for label, value, cls in cards)


def alerts_html(n: int = 10) -> str:
    rows = _recent_alerts(n)
    if not rows:
        return '<li class="empty">No alerts yet — the engine forwards within seconds of a detection.</li>'
    out = []
    for a in rows:
        sev = str(a.get("severity", "INFO"))
        ts = int(_float(a.get("timestamp"))) / 1000
        when = time.strftime("%H:%M:%S", time.localtime(ts)) if ts else "—"
        out.append(
            f'<li class="alert sev-{E(sev.lower())}">'
            f'<span class="chip">{E(sev)}</span>'
            f'<span class="alert-type">{E(str(a.get("alert_type", "?")))}</span>'
            f'<span class="alert-msg">{E(str(a.get("message", "")))}</span>'
            f'<time>{when}</time></li>')
    return "".join(out)


def flagged_html(n: int = 8) -> str:
    rows = _flagged()[:n]
    if not rows:
        return '<li class="empty">No players flagged.</li>'
    out = []
    for p in rows:
        eff = _float(p.get("suspicion_effective"), _float(p.get("suspicion_score")))
        pid = str(p.get("player_id", "?"))
        out.append(
            f'<li><a href="/players/{E(pid)}">{E(pid)}</a>'
            f'<span class="score">{eff:.2f}</span></li>')
    return "".join(out)


def server_rows_html() -> str:
    rows = _servers()
    if not rows:
        return '<tr class="empty"><td colspan="8">No server metrics yet.</td></tr>'
    out = []
    for s in rows:
        hs = _float(s.get("health_score"), 100.0)
        cls = "bad" if hs < 30 else "warn" if hs < 50 else "ok"
        status = str(s.get("status", "?"))
        out.append(
            f'<tr>'
            f'<td class="mono">{E(str(s.get("server_id", "?")))}</td>'
            f'<td>{E(str(s.get("region", "")))}</td>'
            f'<td class="bar-cell"><div class="bar"><div class="bar-fill {cls}" '
            f'style="width:{max(2, min(100, hs)):.0f}%"></div></div>'
            f'<span class="bar-num {cls}">{hs:.1f}</span></td>'
            f'<td><span class="chip {cls}">{E(status)}</span></td>'
            f'<td class="num">{_float(s.get("avg_cpu")):.0f}%</td>'
            f'<td class="num">{_float(s.get("avg_ram")):.0f}%</td>'
            f'<td class="num">{_float(s.get("avg_latency")):.0f} ms</td>'
            f'<td class="num">{E(str(s.get("peak_players", "0")))}</td>'
            f'</tr>')
    return "".join(out)


def servers_stats_html() -> str:
    rows = _servers()
    scores = [_float(s.get("health_score")) for s in rows]
    avg = sum(scores) / len(scores) if scores else 0.0
    degraded = sum(1 for h in scores if h < 50.0)
    return (
        f'<span class="stat"><b>{len(rows)}</b> servers</span>'
        f'<span class="stat"><b>{avg:.1f}</b> avg health</span>'
        f'<span class="stat {"warn" if degraded else ""}"><b>{degraded}</b> degraded</span>'
    )


def cheat_rows_html() -> str:
    rows = _flagged()
    if not rows:
        return '<tr class="empty"><td colspan="7">No flagged players — detections appear within seconds of a suspicious match.</td></tr>'
    out = []
    for p in rows:
        pid = str(p.get("player_id", "?"))
        base = _float(p.get("suspicion_score"))
        eff = _float(p.get("suspicion_effective"), base)
        boost = max(0.0, eff - base)
        iforest = str(p.get("iforest_score") or "—")
        iflag = "yes" if p.get("iforest_flag") == "true" else "—"
        cls = "bad" if eff >= 0.85 else "warn" if eff >= 0.70 else "ok"
        out.append(
            f'<tr>'
            f'<td><a class="mono" href="/players/{E(pid)}">{E(pid)}</a></td>'
            f'<td class="bar-cell"><div class="bar"><div class="bar-fill {cls}" '
            f'style="width:{max(2, min(100, eff * 100)):.0f}%"></div></div>'
            f'<span class="bar-num {cls}">{eff:.2f}</span></td>'
            f'<td class="num">{base:.2f}</td>'
            f'<td class="num {"warn" if boost > 0 else ""}">+{boost:.2f}</td>'
            f'<td class="num">{E(iforest)}</td>'
            f'<td>{"<span class=\'chip warn\'>IF</span>" if iflag == "yes" else "—"}</td>'
            f'<td class="num">{E(str(p.get("total_shots", "0")))}</td>'
            f'</tr>')
    return "".join(out)


def match_rows_html() -> str:
    rows = _matches()
    if not rows:
        return '<tr class="empty"><td colspan="7">No matches scored yet.</td></tr>'
    out = []
    for m in rows[:40]:
        mid = str(m.get("match_id", "?"))
        score = m["quality_score"]
        status = str(m.get("status", "?"))
        cls = "bad" if status == "STOMPED" else "warn" if status == "UNBALANCED" else "ok"
        live = '<span class="dot live" title="live"></span>' if m["active"] else ""
        out.append(
            f'<tr>'
            f'<td><a class="mono" href="/matches/{E(mid)}">{live}{E(mid)}</a></td>'
            f'<td class="bar-cell"><div class="bar"><div class="bar-fill {cls}" '
            f'style="width:{max(2, min(100, score)):.0f}%"></div></div>'
            f'<span class="bar-num {cls}">{score:.1f}</span></td>'
            f'<td><span class="chip {cls}">{E(status)}</span></td>'
            f'<td class="num">{E(str(m.get("total_kills", "0")))}</td>'
            f'<td class="num">{_float(m.get("kill_imbalance")):.2f}</td>'
            f'<td class="num">{E(str(m.get("disconnects", "0")))}</td>'
            f'<td class="num">{E(str(m.get("duration_s", "0")))}s</td>'
            f'</tr>')
    return "".join(out)


def histogram_html() -> str:
    buckets = _histogram_buckets()
    mx = max(buckets) or 1
    total = sum(buckets)
    out = [f'<div class="hist-total">{total} scored matches</div>',
           '<div class="hist-bars">']
    for i, c in enumerate(buckets):
        h = max(3, round(c / mx * 100)) if c else 3
        out.append(
            f'<div class="hbar-wrap"><div class="hbar {"filled" if c else ""}" '
            f'style="height:{h}%">{c if c else ""}</div>'
            f'<span>{i * 10}</span></div>')
    out.append("</div>")
    return "".join(out)


def matches_stats_html() -> str:
    rows = _matches()
    balanced = sum(1 for m in rows if str(m.get("status")) == "BALANCED")
    unbal = sum(1 for m in rows if str(m.get("status")) == "UNBALANCED")
    stomped = sum(1 for m in rows if str(m.get("status")) == "STOMPED")
    active = sum(1 for m in rows if m["active"])
    return (
        f'<span class="stat"><b>{len(rows)}</b> tracked</span>'
        f'<span class="stat"><b>{active}</b> live</span>'
        f'<span class="stat ok"><b>{balanced}</b> balanced</span>'
        f'<span class="stat warn"><b>{unbal}</b> unbalanced</span>'
        f'<span class="stat bad"><b>{stomped}</b> stomped</span>'
    )


def tournament_html() -> str:
    snap = state.tournament_snapshot()
    srv, mt, pl, al = snap["servers"], snap["matches"], snap["players"], snap["alerts"]
    worst = mt["worst"]
    worst_html = (f'Worst match: <a class="mono" href="/matches/{E(worst["match_id"])}">'
                  f'{E(worst["match_id"])}</a> · score {worst["score"]:.1f}'
                  if worst else "No matches scored yet.")
    cards = [
        ("Servers", f'{srv["active"]} active',
         f'avg health {srv["avg_health"] if srv["avg_health"] is not None else "—"} · '
         f'{srv["degraded"]} degraded'),
        ("Matches", f'{mt["scored_total"]} scored', worst_html),
        ("Players", f'{pl["cheat_flagged"]} flagged',
         f'{pl["smurf"]} smurf · {pl["behavior_anomalies"]} behavior shifts'),
        ("Alerts", f'{al["recent_count"]} recent',
         'full history in the <a href="/alerts">alert feed</a>'),
    ]
    return "".join(
        f'<div class="kpi"><div class="kpi-label">{E(label)}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="kpi-sub">{sub}</div></div>'
        for label, value, sub in cards)


def _fmt_event(item: str) -> Optional[str]:
    """Kafka envelope JSON → one live-stream <tr>; None = not displayable."""
    try:
        env = json.loads(item)
        ev = env.get("event") or {}
    except (ValueError, TypeError):
        return None
    topic = str(env.get("topic", ""))
    kind = str(ev.get("event_type") or "?")
    when = time.strftime("%H:%M:%S")

    if topic == "gameplay_events":
        if kind == "SHOT_FIRED":          # far too noisy for humans
            return None
        actor = E(str(ev.get("player_id") or "?"))
        match_id = E(str(ev.get("match_id") or ""))
        if kind in ("KILL", "DAMAGE"):
            tgt = E(str(ev.get("target_player_id") or "?"))
            wep = E(str(ev.get("weapon_id") or ""))
            desc = f"{actor} → {tgt}"
            if kind == "KILL":
                detail = wep + (" · HS" if ev.get("is_headshot") else "")
            else:
                detail = f"{wep} · {int(_float(ev.get('damage')))} dmg"
        else:
            desc, detail = actor, ""
        cls = "k-kill" if kind == "KILL" else "k-dmg" if kind == "DAMAGE" else ""
    elif topic == "player_events":
        pid = E(str(ev.get("player_id") or "?"))
        desc = pid
        detail = E(str((ev.get("metadata") or {}).get("rank", "")))
        match_id = E(str(ev.get("match_id") or ""))
        cls = "k-player"
        if kind == "SHOT_FIRED":
            return None
    elif topic == "server_metrics":
        kind = "SERVER"
        desc = E(str(ev.get("server_id") or "?"))
        detail = (f"cpu {_float(ev.get('cpu_percent')):.0f}% · "
                  f"lat {_float(ev.get('avg_latency_ms')):.0f}ms · "
                  f"{int(_float(ev.get('active_players')))}p")
        match_id = E(str(ev.get("region") or ""))
        cls = "k-server"
    else:
        return None

    return (f'<tr class="{cls}"><td class="mono dim">{when}</td>'
            f'<td><span class="chip kind {cls}">{E(kind)}</span></td>'
            f'<td class="evt-desc">{desc}</td>'
            f'<td class="dim">{E(detail)}</td>'
            f'<td class="mono dim">{match_id}</td></tr>')


# ─── Pages ──────────────────────────────────────────────────────────────────

@router.get("/")
async def view_overview(request: Request):
    return _page(request, "overview.html", "overview", sse="/sse/overview",
                  kpis=kpis_html(),
                  alerts=alerts_html(6),
                  flagged=flagged_html(6))


@router.get("/servers")
async def view_servers(request: Request):
    return _page(request, "servers.html", "servers", sse="/sse/servers",
                  stats=servers_stats_html(), rows=server_rows_html())


@router.get("/anticheat")
async def view_anticheat(request: Request):
    return _page(request, "anticheat.html", "anticheat", sse="/sse/anticheat",
                  rows=cheat_rows_html())


@router.get("/matches")
async def view_matches(request: Request):
    return _page(request, "matches.html", "matches", sse="/sse/matches",
                  stats=matches_stats_html(), rows=match_rows_html(),
                  histogram=histogram_html())


@router.get("/matches/{match_id}")
async def view_match_detail(request: Request, match_id: str):
    r = get_redis_client()
    data = r.hgetall(f"match:{match_id}")
    if not data:
        return _page(request, "notfound.html", "matches",
                      what="match", ident=match_id)
    score = _float(data.get("quality_score"), _float(r.zscore("matches:quality", match_id)))
    return _page(request, "match_detail.html", "matches",
                  m=data, match_id=match_id, score=score)


@router.get("/players/{player_id}")
async def view_player_detail(request: Request, player_id: str):
    r = get_redis_client()
    profile = r.hgetall(f"player:{player_id}")
    smurf = r.hgetall(f"player:smurf:{player_id}")
    behavior = r.hgetall(f"player:behavior:{player_id}")
    if not profile and not smurf and not behavior:
        return _page(request, "notfound.html", "anticheat",
                      what="player", ident=player_id)
    flags = {
        "cheat": r.sismember("players:flagged", player_id),
        "smurf": r.sismember("players:smurf", player_id),
        "behavior": r.sismember("behavior:anomalies", player_id),
    }
    base = _float(profile.get("suspicion_score"))
    eff = _float(profile.get("suspicion_effective"), base)
    return _page(request, "player_detail.html", "anticheat",
                  player_id=player_id, profile=profile, smurf=smurf,
                  behavior=behavior, flags=flags, base=base, eff=eff)


@router.get("/behavior")
async def view_behavior(request: Request):
    r = get_redis_client()
    anomalies = []
    for pid in sorted(r.smembers("behavior:anomalies")):
        data = r.hgetall(f"player:behavior:{pid}")
        if data:
            data["player_id"] = pid
            anomalies.append(data)
    smurfs = []
    for pid in sorted(r.smembers("players:smurf")):
        data = r.hgetall(f"player:smurf:{pid}")
        if data:
            data["player_id"] = pid
            smurfs.append(data)
    smurfs.sort(key=lambda s: _float(s.get("probability")), reverse=True)
    return _page(request, "behavior.html", "behavior",
                  anomalies=anomalies, smurfs=smurfs)


@router.get("/tournament")
async def view_tournament(request: Request):
    return _page(request, "tournament.html", "tournament", sse="/sse/tournament",
                  cards=tournament_html())


@router.get("/alerts")
async def view_alerts(request: Request):
    q = request.query_params
    alert_type = q.get("alert_type") or None
    severity = q.get("severity") or None
    page = max(0, int(q.get("page", "0") or 0))
    PAGE = 20
    try:
        hist = await state.fetch_alert_history(limit=PAGE, offset=page * PAGE,
                                               alert_type=alert_type,
                                               severity=severity)
        hist_error = None
    except Exception as e:
        hist, hist_error = {"alerts": [], "total": 0, "limit": PAGE, "offset": 0}, str(e)

    def _qs(**kw) -> str:
        merged = {"alert_type": alert_type, "severity": severity, "page": 0}
        merged.update({k: v for k, v in kw.items() if v is not None})
        if merged.get("alert_type") is None:
            merged.pop("alert_type")
        if merged.get("severity") is None:
            merged.pop("severity")
        if merged.get("page") in (0, "0", None):
            merged.pop("page", None)
        return "/alerts" + ("?" + "&".join(f"{k}={v}" for k, v in merged.items()) if merged else "")

    total = hist["total"]
    max_page = max(0, (total + PAGE - 1) // PAGE - 1)
    return _page(request, "alerts.html", "alerts", sse="/sse/alerts",
                  live=alerts_html(10),
                  history=hist, hist_error=hist_error,
                  alert_type=alert_type, severity=severity, page=page,
                  max_page=max_page, qs=_qs)


# ─── SSE streams (one per live view) ────────────────────────────────────────

def _stream(gen):
    return DatastarResponse(gen())


@router.get("/sse/overview")
async def sse_overview():
    async def gen():
        q = state.feed.register()
        recent: List[str] = []
        tick = 0
        try:
            while True:
                batch: List[str] = []
                deadline = time.monotonic() + 1.0
                while True:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        break
                    try:
                        item = await asyncio.wait_for(q.get(), timeout=left)
                    except asyncio.TimeoutError:
                        break
                    row = _fmt_event(item)
                    if row:
                        batch.append(row)
                if batch:
                    recent = list(reversed(batch)) + recent
                    recent = recent[:12]
                    yield SSE.patch_elements("".join(recent),
                                             selector="#event-rows",
                                             mode=PatchMode.INNER)
                tick += 1
                if tick % 5 == 0:
                    yield SSE.patch_elements(kpis_html(),
                                             selector="#kpis",
                                             mode=PatchMode.INNER)
                    yield SSE.patch_elements(alerts_html(6),
                                             selector="#recent-alerts",
                                             mode=PatchMode.INNER)
                    yield SSE.patch_elements(flagged_html(6),
                                             selector="#flagged-mini",
                                             mode=PatchMode.INNER)
        finally:
            state.feed.unregister(q)
    return _stream(gen)


@router.get("/sse/servers")
async def sse_servers():
    async def gen():
        while True:
            yield SSE.patch_elements(server_rows_html(),
                                     selector="#server-rows",
                                     mode=PatchMode.INNER)
            yield SSE.patch_elements(servers_stats_html(),
                                     selector="#servers-stats",
                                     mode=PatchMode.INNER)
            await asyncio.sleep(2)
    return _stream(gen)


@router.get("/sse/anticheat")
async def sse_anticheat():
    async def gen():
        while True:
            yield SSE.patch_elements(cheat_rows_html(),
                                     selector="#cheat-rows",
                                     mode=PatchMode.INNER)
            await asyncio.sleep(2)
    return _stream(gen)


@router.get("/sse/matches")
async def sse_matches():
    async def gen():
        while True:
            yield SSE.patch_elements(match_rows_html(),
                                     selector="#match-rows",
                                     mode=PatchMode.INNER)
            yield SSE.patch_elements(histogram_html(),
                                     selector="#histogram",
                                     mode=PatchMode.INNER)
            yield SSE.patch_elements(matches_stats_html(),
                                     selector="#matches-stats",
                                     mode=PatchMode.INNER)
            await asyncio.sleep(2)
    return _stream(gen)


@router.get("/sse/tournament")
async def sse_tournament():
    async def gen():
        while True:
            yield SSE.patch_elements(tournament_html(),
                                     selector="#tournament-cards",
                                     mode=PatchMode.INNER)
            await asyncio.sleep(3)
    return _stream(gen)


@router.get("/sse/alerts")
async def sse_alerts():
    async def gen():
        r = aioredis.Redis(host=state.REDIS_HOST, port=state.REDIS_PORT,
                           decode_responses=True)
        pubsub = r.pubsub()
        await pubsub.subscribe("alerts:stream")
        idle = 0
        try:
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True,
                                               timeout=1.0)
                if msg and msg.get("type") == "message":
                    idle = 0
                    yield SSE.patch_elements(alerts_html(10),
                                             selector="#alert-rows",
                                             mode=PatchMode.INNER)
                else:
                    idle += 1
                    if idle >= 15:        # keepalive re-patch
                        idle = 0
                        yield SSE.patch_elements(alerts_html(10),
                                                 selector="#alert-rows",
                                                 mode=PatchMode.INNER)
        finally:
            try:
                await pubsub.unsubscribe("alerts:stream")
                await pubsub.aclose()
                await r.aclose()
            except Exception:
                pass
    return _stream(gen)
