#!/bin/bash
mkdir -p /tmp/redis-dtg
rm -f /tmp/redis.pid
nohup redis-server --port 6380 --bind 127.0.0.1 --daemonize no --dir /tmp/redis-dtg --save "" --pidfile /tmp/redis.pid > /tmp/redis.log 2>&1 &
disown
sleep 2
echo "PIDFILE:"
cat /tmp/redis.pid 2>&1
echo
redis-cli -p 6380 ping
