package rmq

import (
	"time"

	"github.com/go-redis/redis"
)

type RedisClusterWrapper struct {
	rawClient *redis.ClusterClient
}

func (wrapper RedisClusterWrapper) Set(key string, value string, expiration time.Duration) bool {
	return checkErr(wrapper.rawClient.Set(key, value, expiration).Err())
}

func (wrapper RedisClusterWrapper) Del(key string) (affected int, ok bool) {
	n, err := wrapper.rawClient.Del(key).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisClusterWrapper) TTL(key string) (ttl time.Duration, ok bool) {
	ttl, err := wrapper.rawClient.TTL(key).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return ttl, ok
}

func (wrapper RedisClusterWrapper) LPush(key, value string) bool {
	return checkErr(wrapper.rawClient.LPush(key, value).Err())
}

func (wrapper RedisClusterWrapper) LLen(key string) (affected int, ok bool) {
	n, err := wrapper.rawClient.LLen(key).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisClusterWrapper) LRem(key string, count int, value string) (affected int, ok bool) {
	n, err := wrapper.rawClient.LRem(key, int64(count), value).Result()
	return int(n), checkErr(err)
}

func (wrapper RedisClusterWrapper) LTrim(key string, start, stop int) {
	checkErr(wrapper.rawClient.LTrim(key, int64(start), int64(stop)).Err())
}

func (wrapper RedisClusterWrapper) RPopLPush(source, destination string) (value string, ok bool) {
	value, err := wrapper.rawClient.RPopLPush(source, destination).Result()
	return value, checkErr(err)
}

func (wrapper RedisClusterWrapper) SAdd(key, value string) bool {
	return checkErr(wrapper.rawClient.SAdd(key, value).Err())
}

func (wrapper RedisClusterWrapper) SMembers(key string) []string {
	members, err := wrapper.rawClient.SMembers(key).Result()
	if ok := checkErr(err); !ok {
		return []string{}
	}
	return members
}

func (wrapper RedisClusterWrapper) SRem(key, value string) (affected int, ok bool) {
	n, err := wrapper.rawClient.SRem(key, value).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisClusterWrapper) FlushDb() {
	wrapper.rawClient.FlushDb()
}