#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
PY=${PY:-.venv/bin/python}
LOG=cache/daily-$(date +%F).log; mkdir -p cache
dow=$(date +%u)
dom=$(date +%d)
{
  echo "== $(date)"
  $PY run.py producthunt
  [[ $dow == 1 ]] && $PY run.py launches --limit 300
  [[ $dow == 2 ]] && $PY run.py revenue --limit 200
  $PY run.py chrome-store --limit 3000
  $PY run.py shopify-store --limit 1000
  [[ $dom == 01 ]] && $PY run.py funded --limit 200
  $PY verify.py
  $PY run.py retag
  $PY push_lists.py
  $PY report.py
} >> "$LOG" 2>&1
