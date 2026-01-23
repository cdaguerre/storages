package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/darkweak/storages/core"
	"github.com/pierrec/lz4/v4"
	redis "github.com/redis/rueidis"
	"google.golang.org/protobuf/proto"
)

const (
	// MappingTimestampIndex is the sorted set key that tracks all mapping entries by staleTime.
	// This enables efficient eviction without SCAN operations.
	// See: https://github.com/darkweak/souin/issues/671
	MappingTimestampIndex = "IDX_TIMESTAMPS"
)

// mappingEntry represents a single entry in the mapping timestamp index.
type mappingEntry struct {
	MappingKey string `json:"m"` // The IDX_ prefixed mapping key
	VariedKey  string `json:"v"` // The varied key within the mapping
}

// Redis provider type.
type Redis struct {
	inClient      redis.Client
	stale         time.Duration
	ctx           context.Context
	logger        core.Logger
	configuration redis.ClientOption
	close         func()
	hashtags      string
}

// Factory function create new Redis instance.
func Factory(redisConfiguration core.CacheProvider, logger core.Logger, stale time.Duration) (core.Storer, error) {
	var options redis.ClientOption

	var hashtags string

	redisConfig, err := json.Marshal(redisConfiguration.Configuration)
	if err != nil {
		return nil, err
	}

	if redisConfiguration.Configuration != nil {
		if err := json.Unmarshal(redisConfig, &options); err != nil {
			logger.Infof("Cannot parse your redis configuration: %+v", err)
		}

		if redisConfig, ok := redisConfiguration.Configuration.(map[string]interface{}); ok && redisConfig != nil {
			if value, ok := redisConfig["HashTag"]; ok {
				if v, ok := value.(string); ok {
					hashtags = v
				}
			}
		}
	} else {
		options = redis.ClientOption{
			InitAddress: strings.Split(redisConfiguration.URL, ","),
			SelectDB:    0,
			ClientName:  "souin-redis",
		}
	}

	if options.Dialer.Timeout == 0 {
		options.Dialer.Timeout = time.Second
	}

	if len(options.InitAddress) == 0 {
		return nil, errors.New("no redis addresses given")
	}

	cli, err := redis.NewClient(options)
	if err != nil {
		return nil, err
	}

	return &Redis{
		inClient:      cli,
		ctx:           context.Background(),
		stale:         stale,
		configuration: options,
		logger:        logger,
		close:         cli.Close,
		hashtags:      hashtags,
	}, err
}

// Name returns the storer name.
func (provider *Redis) Name() string {
	return "REDIS"
}

// Uuid returns an unique identifier.
func (provider *Redis) Uuid() string {
	return fmt.Sprintf(
		"%s-%s-%d-%s-%s",
		strings.Join(provider.configuration.InitAddress, ","),
		provider.configuration.Username,
		provider.configuration.SelectDB,
		provider.configuration.ClientName,
		provider.stale,
	)
}

// ListKeys method returns the list of existing keys.
func (provider *Redis) ListKeys() []string {
	var scan redis.ScanEntry

	var err error

	elements := []string{}

	provider.logger.Debugf("Call the ListKeys function in redis")

	for more := true; more; more = scan.Cursor != 0 {
		if scan, err = provider.inClient.Do(context.Background(), provider.inClient.B().Scan().Cursor(scan.Cursor).Match(provider.hashtags+core.MappingKeyPrefix+"*").Build()).AsScanEntry(); err != nil {
			provider.logger.Errorf("Cannot scan: %v", err)
		}

		for _, element := range scan.Elements {
			value := provider.Get(element)

			mapping, err := core.DecodeMapping(value)
			if err != nil {
				continue
			}

			for _, v := range mapping.GetMapping() {
				if v.GetFreshTime().AsTime().Before(time.Now()) && v.GetStaleTime().AsTime().Before(time.Now()) {
					continue
				}

				elements = append(elements, v.GetRealKey())
			}
		}
	}

	return elements
}

// MapKeys method returns the list of existing keys.
func (provider *Redis) MapKeys(prefix string) map[string]string {
	var scan redis.ScanEntry

	var err error

	kvStore := map[string]string{}
	elements := []string{}

	provider.logger.Debugf("Call the MapKeys in redis with the prefix %s", prefix)

	for more := true; more; more = scan.Cursor != 0 {
		if scan, err = provider.inClient.Do(context.Background(), provider.inClient.B().Scan().Cursor(scan.Cursor).Match(prefix+"*").Build()).AsScanEntry(); err != nil {
			provider.logger.Errorf("Cannot scan: %v", err)
		}

		elements = append(elements, scan.Elements...)
	}

	for _, key := range elements {
		k, _ := strings.CutPrefix(key, prefix)
		kvStore[k] = string(provider.Get(key))
	}

	return kvStore
}

// GetMultiLevel tries to load the key and check if one of linked keys is a fresh/stale candidate.
func (provider *Redis) GetMultiLevel(key string, req *http.Request, validator *core.Revalidator) (fresh *http.Response, stale *http.Response) {
	b, e := provider.inClient.Do(provider.ctx, provider.inClient.B().Get().Key(provider.hashtags+core.MappingKeyPrefix+key).Build()).AsBytes()
	if e != nil {
		return
	}

	fresh, stale, _ = core.MappingElection(provider, b, req, validator, provider.logger)

	return
}

// SetMultiLevel tries to store the key with the given value and update the mapping key to store metadata.
func (provider *Redis) SetMultiLevel(baseKey, variedKey string, value []byte, variedHeaders http.Header, etag string, duration time.Duration, realKey string) error {
	now := time.Now()

	compressed := new(bytes.Buffer)
	writer := lz4.NewWriter(compressed)

	defer func() {
		_ = writer.Close()
	}()

	if _, err := writer.ReadFrom(bytes.NewReader(value)); err != nil {
		provider.logger.Errorf("Impossible to compress the key %s into Redis, %v", variedKey, err)

		return err
	}

	if err := provider.inClient.Do(provider.ctx, provider.inClient.B().Set().Key(provider.hashtags+variedKey).Value(compressed.String()).Ex(duration+provider.stale).Build()).Error(); err != nil {
		provider.logger.Errorf("Impossible to set value into Redis, %v", err)

		return err
	}

	mappingKey := provider.hashtags + core.MappingKeyPrefix + baseKey

	v, err := provider.inClient.Do(provider.ctx, provider.inClient.B().Get().Key(mappingKey).Build()).AsBytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	val, err := core.MappingUpdater(provider.hashtags+variedKey, v, provider.logger, now, now.Add(duration), now.Add(duration+provider.stale), variedHeaders, etag, realKey)
	if err != nil {
		return err
	}

	if err = provider.inClient.Do(provider.ctx, provider.inClient.B().Set().Key(mappingKey).Value(string(val)).Build()).Error(); err != nil {
		provider.logger.Errorf("Impossible to set value into Redis, %v", err)
		return err
	}

	// Add entry to the timestamp sorted set for efficient eviction.
	// See: https://github.com/darkweak/souin/issues/671
	staleTime := now.Add(duration + provider.stale).Unix()
	entry := mappingEntry{
		MappingKey: mappingKey,
		VariedKey:  provider.hashtags + variedKey,
	}
	entryJSON, _ := json.Marshal(entry)

	if err = provider.inClient.Do(
		provider.ctx,
		provider.inClient.B().Zadd().
			Key(provider.hashtags+MappingTimestampIndex).
			ScoreMember().
			ScoreMember(float64(staleTime), string(entryJSON)).
			Build(),
	).Error(); err != nil {
		provider.logger.Errorf("Impossible to set value into Redis, %v", err)
	}

	return err
}

// Get method returns the populated response if exists, empty response then.
func (provider *Redis) Get(key string) []byte {
	r, e := provider.inClient.Do(provider.ctx, provider.inClient.B().Get().Key(key).Build()).AsBytes()
	if e != nil && !errors.Is(e, redis.Nil) {
		return nil
	}

	return r
}

// Set method will store the response in Etcd provider.
func (provider *Redis) Set(key string, value []byte, duration time.Duration) error {
	var cmd redis.Completed
	if duration == -1 {
		cmd = provider.inClient.B().Set().Key(key).Value(string(value)).Build()
	} else {
		cmd = provider.inClient.B().Set().Key(key).Value(string(value)).Ex(duration + provider.stale).Build()
	}

	err := provider.inClient.Do(provider.ctx, cmd).Error()
	if err != nil {
		provider.logger.Errorf("Impossible to set value into Redis, %v", err)
	}

	return err
}

// Delete method will delete the response in Etcd provider if exists corresponding to key param.
func (provider *Redis) Delete(key string) {
	_ = provider.inClient.Do(provider.ctx, provider.inClient.B().Del().Key(key).Build())
}

// DeleteMany method will delete the responses in Redis provider if exists corresponding to the regex key param.
func (provider *Redis) DeleteMany(key string) {
	var scan redis.ScanEntry

	var err error

	elements := []string{}

	provider.logger.Debugf("Call the DeleteMany function in redis")

	for more := true; more; more = scan.Cursor != 0 {
		if scan, err = provider.inClient.Do(context.Background(), provider.inClient.B().Scan().Cursor(scan.Cursor).Match(key).Build()).AsScanEntry(); err != nil {
			provider.logger.Errorf("Cannot scan: %v", err)
		}

		elements = append(elements, scan.Elements...)
	}

	_ = provider.inClient.Do(provider.ctx, provider.inClient.B().Del().Key(elements...).Build())
}

// Init method will.
func (provider *Redis) Init() error {
	return nil
}

// Reset method will reset or close provider.
func (provider *Redis) Reset() error {
	_ = provider.inClient.Do(provider.ctx, provider.inClient.B().Flushdb().Build())

	return nil
}

func (provider *Redis) Reconnect() {
	provider.logger.Debug("Doing nothing on reconnect because rueidis handles it!")
}

// EvictExpiredMappingEntries removes expired entries from mapping keys using sorted sets.
// This method avoids expensive SCAN operations by using ZRANGEBYSCORE.
// Returns true to indicate that eviction was handled (no fallback to SCAN needed).
// See: https://github.com/darkweak/souin/issues/671
func (provider *Redis) EvictExpiredMappingEntries() bool {
	now := time.Now().Unix()
	nowStr := strconv.FormatInt(now, 10)

	// Get all expired entries (score <= now) from the sorted set
	expiredEntries, err := provider.inClient.Do(
		provider.ctx,
		provider.inClient.B().Zrangebyscore().
			Key(provider.hashtags+MappingTimestampIndex).
			Min("-inf").
			Max(nowStr).
			Build(),
	).AsStrSlice()

	if err != nil {
		provider.logger.Errorf("Error getting expired mapping entries: %v", err)
		return true // Still return true to avoid SCAN fallback
	}

	if len(expiredEntries) == 0 {
		return true
	}

	// Process expired entries
	for _, entryJSON := range expiredEntries {
		var entry mappingEntry
		if err := json.Unmarshal([]byte(entryJSON), &entry); err != nil {
			provider.logger.Errorf("Error unmarshaling mapping entry: %v", err)
			continue
		}

		// Load the mapping and remove the expired varied key
		provider.removeExpiredEntryFromMapping(entry.MappingKey, entry.VariedKey)
	}

	// Remove all processed entries from the sorted set
	if len(expiredEntries) > 0 {
		err = provider.inClient.Do(
			provider.ctx,
			provider.inClient.B().Zrem().
				Key(provider.hashtags+MappingTimestampIndex).
				Member(expiredEntries...).
				Build(),
		).Error()

		if err != nil {
			provider.logger.Errorf("Error removing expired entries from sorted set: %v", err)
		}
	}

	return true
}

// removeExpiredEntryFromMapping removes a specific varied key from a mapping.
func (provider *Redis) removeExpiredEntryFromMapping(mappingKey, variedKey string) {
	// Get the current mapping
	item, err := provider.inClient.Do(
		provider.ctx,
		provider.inClient.B().Get().Key(mappingKey).Build(),
	).AsBytes()

	if err != nil || len(item) == 0 {
		return
	}

	mapping := &core.StorageMapper{}
	if err := proto.Unmarshal(item, mapping); err != nil {
		// Corrupted mapping, delete it
		provider.Delete(mappingKey)
		return
	}

	// Remove the expired entry
	if _, exists := mapping.GetMapping()[variedKey]; exists {
		delete(mapping.Mapping, variedKey)

		if len(mapping.GetMapping()) == 0 {
			// No more entries, delete the entire mapping
			provider.Delete(mappingKey)
		} else {
			// Re-save the mapping with the entry removed
			val, err := proto.Marshal(mapping)
			if err != nil {
				provider.logger.Errorf("Error marshaling mapping: %v", err)
				return
			}

			_ = provider.inClient.Do(
				provider.ctx,
				provider.inClient.B().Set().
					Key(mappingKey).
					Value(string(val)).
					Build(),
			)
		}
	}
}
