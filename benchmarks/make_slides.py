#!/usr/bin/env python3
"""Render demo-video slides (1280x720) for docs/demo.mp4. Stdlib + Pillow only."""
import os
from PIL import Image, ImageDraw, ImageFont

W, H = 1280, 720
OUT = os.path.join(os.path.dirname(__file__), "slides")
os.makedirs(OUT, exist_ok=True)

BG, FG, MUTED, ACCENT = (16, 18, 24), (236, 238, 242), (150, 156, 168), (110, 168, 254)
FONT_DIRS = ["/usr/share/fonts/TTF", "/usr/share/fonts/truetype"]


def _find(name):
    for d in FONT_DIRS:
        p = os.path.join(d, name)
        if os.path.exists(p):
            return p
    return None


def font(size, bold=False):
    for name in (("DejaVuSans-Bold.ttf", "DejaVuSans.ttf") if bold
                 else ("DejaVuSans.ttf",)):
        p = _find(name)
        if p:
            return ImageFont.truetype(p, size)
    return ImageFont.load_default(size)


def wrap(draw, text, f, width):
    lines, cur = [], ""
    for word in text.split():
        t = (cur + " " + word).strip()
        if draw.textlength(t, font=f) <= width:
            cur = t
        else:
            lines.append(cur)
            cur = word
    if cur:
        lines.append(cur)
    return lines


def base(title, kicker):
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, W, 6], fill=ACCENT)
    d.text((50, 36), kicker.upper(), font=font(18, True), fill=ACCENT)
    d.text((50, 66), title, font=font(40, True), fill=FG)
    d.text((50, H - 34), "Gaming Intelligence Platform · BTP · Karthik Das P",
           font=font(16), fill=MUTED)
    return img, d


def bullets(d, items, x, y, width, size=22, gap=10):
    f = font(size)
    for it in items:
        d.ellipse([x, y + 8, x + 7, y + 15], fill=ACCENT)
        for ln in wrap(d, it, f, width - 30):
            d.text((x + 22, y), ln, font=f, fill=FG)
            y += size + 6
        y += gap
    return y


def paste_fit(img, path, box):
    x, y, bw, bh = box
    im = Image.open(path).convert("RGB")
    im.thumbnail((bw, bh))
    img.paste(im, (x + (bw - im.width) // 2, y + (bh - im.height) // 2))
    return im.width, im.height


SHOTS = os.path.join(os.path.dirname(__file__), "..", "docs", "screenshots")
BENCH = os.path.join(os.path.dirname(__file__), "..", "docs", "benchmarks.png")

def slide1():
    img, d = base("Gaming Intelligence Platform", "Demo · segment 1 of 6")
    bullets(d, [
        "Real-time competitive gaming analytics, end to end:",
        "Go simulator → Kafka → PySpark Structured Streaming → Redis / PostgreSQL",
        "served by FastAPI with a live Datastar dashboard at :8000.",
        "Six anomaly queries: server health, match quality, behavior drift,",
        "cheat detection, smurf detection, and economy analytics.",
        "Everything in this video is live data from the running stack.",
    ], 50, 160, 1180, size=26, gap=14)
    paste_fit(img, os.path.join(SHOTS, "overview.png"), (140, 420, 1000, 260))
    img.save(os.path.join(OUT, "slide1.png"))

def slide2():
    img, d = base("Live dashboard — Overview", "Demo · segment 2 of 6")
    paste_fit(img, os.path.join(SHOTS, "overview.png"), (40, 140, 1200, 540))
    d.text((50, 110), "KPIs, event stream, alerts and flagged players — refreshed over SSE every 2 s",
           font=font(20), fill=MUTED)
    img.save(os.path.join(OUT, "slide2.png"))

def slide3():
    img, d = base("Server health & anti-cheat", "Demo · segment 3 of 6")
    paste_fit(img, os.path.join(SHOTS, "servers.png"), (30, 150, 620, 300))
    paste_fit(img, os.path.join(SHOTS, "anticheat.png"), (650, 150, 600, 300))
    d.text((40, 470), "Left: health bars with server-02 the deliberately degraded node (CRITICAL ≤ 70).",
           font=font(21), fill=FG)
    d.text((40, 505), "Right: flagged players ranked by suspicion = 0.45·acc + 0.40·hs + 0.15·rxn,",
           font=font(21), fill=FG)
    d.text((40, 540), "boosted +0.15 by the per-player CUSUM drift detector, cross-checked by IsolationForest.",
           font=font(21), fill=FG)
    img.save(os.path.join(OUT, "slide3.png"))

def slide4():
    img, d = base("Match quality & alert history", "Demo · segment 4 of 6")
    paste_fit(img, os.path.join(SHOTS, "matches.png"), (30, 150, 620, 300))
    paste_fit(img, os.path.join(SHOTS, "alerts.png"), (650, 150, 600, 300))
    d.text((40, 470), "Left: session-window match scores with drill-down. Right: alerts pushed by the Go engine,",
           font=font(21), fill=FG)
    d.text((40, 505), "deduplicated in two layers, persisted to PostgreSQL, filterable by severity, type and date.",
           font=font(21), fill=FG)
    img.save(os.path.join(OUT, "slide4.png"))

def slide5():
    img, d = base("Benchmark: 5 tiers, 1k → 20k events/s", "Demo · segment 5 of 6")
    paste_fit(img, BENCH, (40, 130, 860, 480))
    bullets(d, [
        "20,695 ev/s absorbed,",
        "worst lag 10.9 s, drained ≤ 7 s",
        "zero lag through 5k ev/s",
        "graceful backpressure",
        "CPU-bound, ≤ 1.7 GB memory",
        "64 automated tests green",
    ], 930, 160, 320, size=20, gap=8)
    img.save(os.path.join(OUT, "slide5.png"))

def slide6():
    img, d = base("Batch layer & wrap-up", "Demo · segment 6 of 6")
    bullets(d, [
        "Six offline PySpark jobs over the 94 MB Parquet archive:",
        "skill progression, weapon meta, cheat precision/recall vs ground truth,",
        "match-quality histogram, server reliability ranking, peak hours.",
        "",
        "Phases 0–7 complete: environment, simulator, ingestion, streaming,",
        "API, dashboard, benchmarking, documentation — with a full BTP report.",
        "",
        "Repo: gaming-intelligence-platform · report: docs/BTP_REPORT.md",
    ], 50, 170, 1180, size=26, gap=14)
    img.save(os.path.join(OUT, "slide6.png"))

for fn in (slide1, slide2, slide3, slide4, slide5, slide6):
    fn()
print("slides:", sorted(os.listdir(OUT)))