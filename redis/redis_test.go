package redis_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/darkweak/storages/core"
	"github.com/darkweak/storages/redis"
	"go.uber.org/zap"
)

const (
	byteKey        = "MyByteKey"
	nonExistentKey = "NonExistentKey"
	baseValue      = "My first data"
)

func getRedisInstance() (core.Storer, error) {
	return redis.Factory(core.CacheProvider{URL: "localhost:6379"}, zap.NewNop().Sugar(), 0)
}

func TestRedisConnectionFactory(t *testing.T) {
	instance, err := getRedisInstance()
	if nil != err {
		t.Error("Shouldn't have panic", err)
	}

	if nil == instance {
		t.Error("Redis should be instanciated")
	}
}

func TestIShouldBeAbleToReadAndWriteDataInRedis(t *testing.T) {
	client, _ := getRedisInstance()

	_ = client.Set("Test", []byte(baseValue), time.Duration(20)*time.Second)
	time.Sleep(1 * time.Second)

	res := client.Get("Test")
	if len(res) == 0 {
		t.Errorf("Key %s should exist", baseValue)
	}

	if baseValue != string(res) {
		t.Errorf("%s not corresponding to %s", string(res), baseValue)
	}
}

func TestRedis_GetRequestInCache(t *testing.T) {
	client, _ := getRedisInstance()
	res := client.Get(nonExistentKey)

	if 0 < len(res) {
		t.Errorf("Key %s should not exist", nonExistentKey)
	}
}

func TestRedis_GetSetRequestInCache_OneByte(t *testing.T) {
	client, _ := getRedisInstance()
	_ = client.Set(byteKey, []byte("A"), time.Duration(20)*time.Second)
	time.Sleep(1 * time.Second)

	res := client.Get(byteKey)
	if len(res) == 0 {
		t.Errorf("Key %s should exist", byteKey)
	}

	if string(res) != "A" {
		t.Errorf("%s not corresponding to %v", res, 65)
	}
}

func TestRedis_SetRequestInCache_TTL(t *testing.T) {
	key := "MyEmptyKey"
	client, _ := getRedisInstance()
	val := []byte("Hello world")
	_ = client.Set(key, val, time.Duration(20)*time.Second)
	time.Sleep(1 * time.Second)

	newValue := client.Get(key)

	if len(newValue) != len(val) {
		t.Errorf("Key %s should be equals to %s, %s provided", key, val, newValue)
	}
}

func TestRedis_DeleteRequestInCache(t *testing.T) {
	client, _ := getRedisInstance()
	client.Delete(byteKey)
	time.Sleep(1 * time.Second)

	if 0 < len(client.Get(byteKey)) {
		t.Errorf("Key %s should not exist", byteKey)
	}
}

func TestRedis_Init(t *testing.T) {
	client, _ := getRedisInstance()
	err := client.Init()

	if nil != err {
		t.Error("Impossible to init Redis provider")
	}
}

const maxCounter = 10

func TestRedis_MapKeys(t *testing.T) {
	client, _ := getRedisInstance()
	prefix := "MAP_KEYS_PREFIX_"

	keys := client.MapKeys(prefix)
	if len(keys) != 0 {
		t.Error("The map should be empty")
	}

	for i := range maxCounter {
		_ = client.Set(fmt.Sprintf("%s%d", prefix, i), []byte(fmt.Sprintf("Hello from %d", i)), time.Second)
	}

	keys = client.MapKeys(prefix)
	if len(keys) != maxCounter {
		t.Errorf("The map should contain %d elements, %d given", maxCounter, len(keys))
	}

	for k, v := range keys {
		if v != "Hello from "+k {
			t.Errorf("Expected Hello from %s, %s given", k, v)
		}
	}
}

func TestRedis_DeleteMany(t *testing.T) {
	client, _ := getRedisInstance()

	if len(client.MapKeys("")) != 12 {
		t.Error("The map should contain 12 elements")
	}

	client.DeleteMany("MAP_KEYS_PREFIX_*")

	if len(client.MapKeys("")) != 2 {
		t.Error("The map should contain 2 element")
	}

	client.DeleteMany("*")

	if len(client.MapKeys("")) != 0 {
		t.Error("The map should be empty")
	}
}

func TestRedis_EvictExpiredMappingEntries(t *testing.T) {
	client, err := getRedisInstance()
	if err != nil {
		t.Fatalf("Failed to create Redis instance: %v", err)
	}

	// Clean up before test
	client.DeleteMany("*")

	// Cast to access the EvictExpiredMappingEntries method
	redisClient, ok := client.(*redis.Redis)
	if !ok {
		t.Fatal("Failed to cast to *redis.Redis")
	}

	// Test that EvictExpiredMappingEntries returns true (indicating it handles eviction)
	result := redisClient.EvictExpiredMappingEntries()
	if !result {
		t.Error("EvictExpiredMappingEntries should return true")
	}

	// Clean up after test
	client.DeleteMany("*")
}

func TestRedis_EvictExpiredMappingEntries_WithExpiredEntries(t *testing.T) {
	client, err := getRedisInstance()
	if err != nil {
		t.Fatalf("Failed to create Redis instance: %v", err)
	}

	// Clean up before test
	client.DeleteMany("*")

	// Use SetMultiLevel to create a mapping entry with a short but valid duration
	// This will add an entry to the IDX_TIMESTAMPS sorted set
	baseKey := "test-evict-base"
	variedKey := "test-evict-varied"
	value := []byte("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\ntest body")

	// Use 1 second duration - Redis requires at least 1 second for expiry
	err = client.SetMultiLevel(baseKey, variedKey, value, nil, "", 1*time.Second, "test-real-key")
	if err != nil {
		t.Fatalf("Failed to set multi-level: %v", err)
	}

	// Verify the mapping was created
	mappingKey := core.MappingKeyPrefix + baseKey
	mappingValue := client.Get(mappingKey)
	if len(mappingValue) == 0 {
		t.Error("Mapping should have been created")
	}

	// Wait for the entry to expire (stale time = duration + stale, but stale is 0 in test)
	time.Sleep(2 * time.Second)

	// Cast to access the EvictExpiredMappingEntries method
	redisClient, ok := client.(*redis.Redis)
	if !ok {
		t.Fatal("Failed to cast to *redis.Redis")
	}

	// Run eviction
	result := redisClient.EvictExpiredMappingEntries()
	if !result {
		t.Error("EvictExpiredMappingEntries should return true")
	}

	// The mapping key should be deleted since it only had one entry
	mappingValue = client.Get(mappingKey)
	if len(mappingValue) != 0 {
		t.Error("Mapping should have been deleted after eviction")
	}

	// Clean up after test
	client.DeleteMany("*")
}

func TestRedis_EvictExpiredMappingEntries_NoExpiredEntries(t *testing.T) {
	client, err := getRedisInstance()
	if err != nil {
		t.Fatalf("Failed to create Redis instance: %v", err)
	}

	// Clean up before test
	client.DeleteMany("*")

	// Use SetMultiLevel to create a mapping entry with a long duration
	baseKey := "test-no-evict-base"
	variedKey := "test-no-evict-varied"
	value := []byte("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\ntest body")

	err = client.SetMultiLevel(baseKey, variedKey, value, nil, "", 1*time.Hour, "test-real-key")
	if err != nil {
		t.Fatalf("Failed to set multi-level: %v", err)
	}

	// Verify the mapping was created
	mappingKey := core.MappingKeyPrefix + baseKey
	mappingValue := client.Get(mappingKey)
	if len(mappingValue) == 0 {
		t.Error("Mapping should have been created")
	}

	// Cast to access the EvictExpiredMappingEntries method
	redisClient, ok := client.(*redis.Redis)
	if !ok {
		t.Fatal("Failed to cast to *redis.Redis")
	}

	// Run eviction - should not remove anything since entry hasn't expired
	result := redisClient.EvictExpiredMappingEntries()
	if !result {
		t.Error("EvictExpiredMappingEntries should return true")
	}

	// The mapping should still exist
	mappingValue = client.Get(mappingKey)
	if len(mappingValue) == 0 {
		t.Error("Mapping should still exist after eviction of non-expired entries")
	}

	// Clean up after test
	client.DeleteMany("*")
}
