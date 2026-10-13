#!/usr/bin/env bash
# Apply per-network egress delay, then exec the command.
# NETEM_RULES="172.31.11.:15 172.31.12.:35"  (address prefix : one-way delay ms)
set -euo pipefail
for rule in ${NETEM_RULES:-}; do
  prefix=${rule%%:*}; delay=${rule##*:}
  iface=$(ip -o -4 addr show | awk -v p="$prefix" 'index($4,p)==1 {print $2; exit}')
  [ -n "$iface" ] || { echo "netem: no interface for $prefix" >&2; exit 1; }
  if [ "$delay" -gt 0 ]; then
    tc qdisc replace dev "$iface" root netem delay "${delay}ms"
  else
    tc qdisc del dev "$iface" root 2>/dev/null || true
  fi
  echo "netem: $iface ($prefix) +${delay}ms"
done
exec "$@"
