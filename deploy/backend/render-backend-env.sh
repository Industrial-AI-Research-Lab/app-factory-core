#!/usr/bin/env bash
# Render backend.<env>.env from the environment. The key list is taken from the template, so the
# rendered file always matches the committed schema; each value comes from the like-named env var
# (a GitHub Secret or Variable), empty if unset. printf keeps '=', '@', '?', '&' etc. verbatim.
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <template> <out-file>" >&2
  exit 2
fi

template=$1
out=$2
if [ ! -f "$template" ]; then
  echo "Template not found: $template" >&2
  exit 2
fi

umask 077
tmp="${out}.tmp.$$"
: > "$tmp"
while IFS= read -r key; do
  printf '%s=%s\n' "$key" "${!key-}" >> "$tmp"
done < <(grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "$template" | cut -d= -f1)
mv "$tmp" "$out"

echo "Rendered $out ($(wc -l < "$out" | tr -d ' ') keys)"
