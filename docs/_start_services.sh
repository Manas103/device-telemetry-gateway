#!/bin/bash
set -x
pgrep -af clickhouse
echo ---REDIS---
pgrep -af redis-server
echo ---PORTS---
ss -ltnp 2>&1 | grep -E "8123|900|6379|6380"
