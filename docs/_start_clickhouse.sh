#!/bin/bash
cd ~/build/clickhouse || exit 1
rm -f /tmp/clickhouse.pid
nohup ./clickhouse server --pidfile=/tmp/clickhouse.pid -- --path=/tmp/ch-data/ --http_port=8123 --tcp_port=9006 --mysql_port=9105 --interserver_http_port=9107 --listen_host=127.0.0.1 > /tmp/clickhouse.log 2>&1 &
disown
sleep 6
echo "PIDFILE:"
cat /tmp/clickhouse.pid 2>&1
echo
echo "PING:"
curl -s http://127.0.0.1:8123/ping
echo
echo "LOG TAIL:"
tail -15 /tmp/clickhouse.log
