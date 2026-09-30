#!/usr/bin/env bash
# Assemble docs/demo.mp4 from benchmarks/slides/slideN.png + narration segN.mp3.
# Each slide holds for its narration + pad; pad stretches the total to ~5.5 min.
set -euo pipefail

FF="$(command -v ffmpeg)"
FP="$(command -v ffprobe)"
PROJ="$(cd "$(dirname "$0")/.." && pwd)"
SLIDES="$PROJ/benchmarks/slides"
AUD="$HOME/.hermes/profiles/big-data/cache/scratch/demo"
WORK="$AUD/clips"
TARGET=330   # desired total seconds
mkdir -p "$WORK"

dur() { "$FP" -v error -show_entries format=duration -of csv=p=0 "$1"; }

total=0
for n in 1 2 3 4 5 6; do
  d="$(dur "$AUD/seg$n.mp3")"
  total="$(awk -v a="$total" -v b="$d" 'BEGIN{printf "%d", a+b}')"
done
extra=$(( TARGET - total ))
pad=$(( extra > 0 ? extra / 6 : 2 ))
[ "$pad" -lt 2 ] && pad=2
echo "audio total=${total}s pad=${pad}s/slide"

for n in 1 2 3 4 5 6; do
  a="$(dur "$AUD/seg$n.mp3")"
  hold="$(awk -v a="$a" -v p="$pad" 'BEGIN{printf "%.2f", a+p}')"
  "$FF" -y -v error -loop 1 -framerate 30 \
    -i "$SLIDES/slide$n.png" -i "$AUD/seg$n.mp3" \
    -c:v libx264 -preset veryfast -crf 21 -pix_fmt yuv420p \
    -c:a aac -b:a 128k -ar 44100 -ac 2 \
    -t "$hold" "$WORK/clip$n.mp4"
  echo "clip$n: ${hold}s"
done

: > "$WORK/list.txt"
for n in 1 2 3 4 5 6; do echo "file 'clip$n.mp4'" >> "$WORK/list.txt"; done
"$FF" -y -v error -f concat -safe 0 -i "$WORK/list.txt" -c copy "$PROJ/docs/demo.mp4"

final="$(dur "$PROJ/docs/demo.mp4")"
sz="$(du -h "$PROJ/docs/demo.mp4" | cut -f1)"
echo "demo.mp4: ${final}s, ${sz}"