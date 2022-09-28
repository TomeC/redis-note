#!/bin/bash
/root/redis-5.0.8/src/redis-server ./6379/redis.conf
/root/redis-5.0.8/src/redis-server ./6378/redis.conf
/root/redis-5.0.8/src/redis-server ./6377/redis.conf
sleep 2
/root/redis-5.0.8/src/redis-sentinel ./26377/sentinel.conf
/root/redis-5.0.8/src/redis-sentinel ./26378/sentinel.conf
/root/redis-5.0.8/src/redis-sentinel ./26379/sentinel.conf

