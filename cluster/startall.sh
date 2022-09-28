#!/bin/bash
/root/redis-5.0.8/src/redis-server ./6374/redis.conf
/root/redis-5.0.8/src/redis-server ./6375/redis.conf
/root/redis-5.0.8/src/redis-server ./6376/redis.conf
/root/redis-5.0.8/src/redis-server ./6377/redis.conf
/root/redis-5.0.8/src/redis-server ./6378/redis.conf
/root/redis-5.0.8/src/redis-server ./6379/redis.conf

sleep 5
#../src/redis-cli --cluster create 192.168.67.130:6374 192.168.67.130:6375 192.168.67.130:6376 192.168.67.130:6377 192.168.67.130:6378 192.168.67.130:6379 --cluster-replicas 1
