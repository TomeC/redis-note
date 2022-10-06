#!/bin/bash
cd 6374 && ../../src/redis-server ./redis.conf
cd ../6375 && ../../src/redis-server ./redis.conf
cd ../6376 && ../../src/redis-server ./redis.conf
cd ../6377 && ../../src/redis-server ./redis.conf
cd ../6378 && ../../src/redis-server ./redis.conf
cd ../6379 && ../../src/redis-server ./redis.conf

sleep 5
#../src/redis-cli --cluster create 192.168.67.130:6374 192.168.67.130:6375 192.168.67.130:6376 192.168.67.130:6377 192.168.67.130:6378 192.168.67.130:6379 --cluster-replicas 1
