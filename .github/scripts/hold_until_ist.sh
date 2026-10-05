#!/usr/bin/env bash
# Usage: hold_until_ist.sh START_HH:MM TARGET_HH:MM
#
# GitHub starts `schedule` runs hours late and unpredictably, so the slots
# are scheduled EARLY and this script holds an early-starting run until the
# real target time (IST). If the run starts inside [START, TARGET) IST it
# sleeps until TARGET; if it starts at/after TARGET (the usual case when
# GitHub is late) it returns immediately. Test hooks: HOLD_NOW_EPOCH fakes
# the clock, HOLD_DRY_RUN skips the actual sleep.
set -euo pipefail

start="$1"
target="$2"
now_epoch="${HOLD_NOW_EPOCH:-$(date -u +%s)}"
sod=$(( (now_epoch + 19800) % 86400 ))   # seconds since IST midnight

hm_to_s() { local h m; IFS=: read -r h m <<<"$1"; echo $(( 10#$h * 3600 + 10#$m * 60 )); }
s=$(hm_to_s "$start")
t=$(hm_to_s "$target")

if [ "$sod" -ge "$s" ] && [ "$sod" -lt "$t" ]; then
  wait=$(( t - sod ))
  echo "Run started $(( wait / 60 )) min before ${target} IST -- holding until then."
  [ -n "${HOLD_DRY_RUN:-}" ] && { echo "WAIT_SECONDS=${wait}"; exit 0; }
  sleep "$wait"
else
  echo "Run started at/after ${target} IST (or outside the hold window) -- no hold needed."
  echo "WAIT_SECONDS=0"
fi
