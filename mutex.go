package mutex_redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/infrago/infra"
	"github.com/infrago/mutex"
	"github.com/redis/go-redis/v9"
)

type lockedError struct{}
type lostLockError struct{}
type timeoutError struct{}
type closedError struct{}
type invalidLeaseError struct{}

func (lockedError) Error() string { return "mutex already locked" }
func (lockedError) Is(target error) bool {
	return target != nil && target.Error() == "mutex already locked"
}
func (lostLockError) Error() string { return "mutex lock is lost" }
func (lostLockError) Is(target error) bool {
	return target != nil && target.Error() == "mutex lock is lost"
}
func (timeoutError) Error() string { return "mutex timeout" }
func (timeoutError) Is(target error) bool {
	return target != nil && target.Error() == "mutex timeout"
}
func (closedError) Error() string { return "mutex is closed" }
func (closedError) Is(target error) bool {
	return target != nil && target.Error() == "mutex is closed"
}
func (invalidLeaseError) Error() string { return "invalid mutex lease" }
func (invalidLeaseError) Is(target error) bool {
	return target != nil && target.Error() == "invalid mutex lease"
}

var errLocked error = lockedError{}
var errLostLock error = lostLockError{}
var errTimeout error = timeoutError{}
var errClosed error = closedError{}
var errInvalidLease error = invalidLeaseError{}

type (
	redisDriver struct{}

	redisConnect struct {
		instance *mutex.Instance
		setting  redisSetting
		client   *redis.Client
		mu       sync.Mutex
		tokens   map[string]tokenState
		stop     chan struct{}
		done     chan struct{}
		opened   bool
		closed   bool
	}

	tokenState struct {
		token string
		until time.Time
	}

	redisSetting struct {
		Addr            string
		Username        string
		Password        string
		Database        int
		Timeout         time.Duration
		CleanupInterval time.Duration
	}
)

const unlockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`

const refreshScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`

func init() {
	infra.Register("redis", &redisDriver{})
}

func (d *redisDriver) Connect(inst *mutex.Instance) (mutex.Connection, error) {
	setting := redisSetting{
		Addr:            "127.0.0.1:6379",
		Timeout:         3 * time.Second,
		CleanupInterval: 30 * time.Second,
	}

	if v, ok := inst.Config.Setting["addr"].(string); ok && v != "" {
		setting.Addr = v
	}
	if v, ok := inst.Config.Setting["server"].(string); ok && v != "" {
		setting.Addr = v
	}
	if v, ok := inst.Config.Setting["username"].(string); ok && v != "" {
		setting.Username = v
	}
	if v, ok := inst.Config.Setting["user"].(string); ok && v != "" {
		setting.Username = v
	}
	if v, ok := inst.Config.Setting["password"].(string); ok {
		setting.Password = v
	}
	if v, ok := inst.Config.Setting["pass"].(string); ok {
		setting.Password = v
	}

	if v, ok := inst.Config.Setting["database"]; ok {
		switch vv := v.(type) {
		case int:
			setting.Database = vv
		case int64:
			setting.Database = int(vv)
		case float64:
			setting.Database = int(vv)
		case string:
			if num, err := strconv.Atoi(vv); err == nil {
				setting.Database = num
			}
		}
	}
	if v, ok := parseDuration(inst.Config.Setting["timeout"]); ok && v > 0 {
		setting.Timeout = v
	}
	if v, ok := parseDuration(inst.Config.Setting["cleanup_interval"]); ok && v > 0 {
		setting.CleanupInterval = v
	}
	if v := configDuration(inst, "CleanupInterval"); v > 0 {
		setting.CleanupInterval = v
	}
	if setting.CleanupInterval <= 0 {
		setting.CleanupInterval = 30 * time.Second
	}

	return &redisConnect{
		instance: inst,
		setting:  setting,
		tokens:   map[string]tokenState{},
	}, nil
}

func (c *redisConnect) Open() error {
	c.client = redis.NewClient(&redis.Options{
		Addr:     c.setting.Addr,
		Username: c.setting.Username,
		Password: c.setting.Password,
		DB:       c.setting.Database,
	})
	ctx, cancel := c.context()
	defer cancel()
	if err := normalizeError(c.client.Ping(ctx).Err()); err != nil {
		return err
	}
	c.opened = true
	c.closed = false
	c.startCleaner()
	return nil
}

func (c *redisConnect) Close() error {
	c.stopCleaner()
	c.mu.Lock()
	c.tokens = map[string]tokenState{}
	c.opened = false
	c.closed = true
	c.mu.Unlock()
	if c.client != nil {
		err := c.client.Close()
		c.client = nil
		return err
	}
	return nil
}

func (c *redisConnect) Lock(key string, expire time.Duration) error {
	_, err := c.LockToken(key, expire)
	return err
}

func (c *redisConnect) LockToken(key string, expire time.Duration) (string, error) {
	if err := c.stateError(); err != nil {
		return "", err
	}
	c.cleanupExpiredTokens()
	if expire < 0 {
		return "", errInvalidLease
	}
	if expire <= 0 {
		expire = c.instance.Config.Expire
	}
	if expire <= 0 {
		expire = time.Second
	}

	token, err := randToken()
	if err != nil {
		return "", err
	}
	ctx, cancel := c.context()
	defer cancel()
	ok, err := c.client.SetNX(ctx, key, token, expire).Result()
	if err != nil {
		return "", normalizeError(err)
	}
	if !ok {
		return "", errLocked
	}
	c.mu.Lock()
	c.tokens[key] = tokenState{token: token, until: time.Now().Add(expire)}
	c.mu.Unlock()
	return token, nil
}

func (c *redisConnect) Unlock(key string) error {
	return c.UnlockToken(key, "")
}

func (c *redisConnect) UnlockToken(key, token string) error {
	if err := c.stateError(); err != nil {
		return err
	}
	c.cleanupExpiredTokens()

	c.mu.Lock()
	current, ok := c.tokens[key]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	if token != "" && token != current.token {
		c.mu.Unlock()
		return nil
	}
	delete(c.tokens, key)
	c.mu.Unlock()
	ctx, cancel := c.context()
	defer cancel()
	_, err := c.client.Eval(ctx, unlockScript, []string{key}, current.token).Result()
	return normalizeError(err)
}

func (c *redisConnect) Refresh(key string, expire time.Duration) error {
	return c.RefreshToken(key, "", expire)
}

func (c *redisConnect) RefreshToken(key, token string, expire time.Duration) error {
	if err := c.stateError(); err != nil {
		return err
	}
	c.cleanupExpiredTokens()
	if expire < 0 {
		return errInvalidLease
	}
	if expire <= 0 {
		expire = c.instance.Config.Expire
	}
	if expire <= 0 {
		expire = time.Second
	}

	c.mu.Lock()
	current, ok := c.tokens[key]
	if !ok {
		c.mu.Unlock()
		return errLostLock
	}
	if token != "" && token != current.token {
		c.mu.Unlock()
		return errLostLock
	}
	c.mu.Unlock()

	ctx, cancel := c.context()
	defer cancel()
	res, err := c.client.Eval(ctx, refreshScript, []string{key}, current.token, expire.Milliseconds()).Int64()
	if err != nil {
		return normalizeError(err)
	}
	if res == 0 {
		c.mu.Lock()
		delete(c.tokens, key)
		c.mu.Unlock()
		return errLostLock
	}

	c.mu.Lock()
	current.until = time.Now().Add(expire)
	c.tokens[key] = current
	c.mu.Unlock()
	return nil
}

func (c *redisConnect) Locked(key string) (bool, error) {
	if err := c.stateError(); err != nil {
		return false, err
	}
	c.cleanupExpiredTokens()
	ctx, cancel := c.context()
	defer cancel()
	cnt, err := c.client.Exists(ctx, key).Result()
	if err == nil && cnt == 0 {
		c.mu.Lock()
		delete(c.tokens, key)
		c.mu.Unlock()
	}
	return cnt > 0, normalizeError(err)
}

func (c *redisConnect) context() (context.Context, context.CancelFunc) {
	if c.setting.Timeout <= 0 {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), c.setting.Timeout)
}

func (c *redisConnect) cleanupExpiredTokens() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	removed := 0
	for key, state := range c.tokens {
		if !state.until.After(now) {
			delete(c.tokens, key)
			removed++
		}
	}
	return removed
}

func (c *redisConnect) startCleaner() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stop != nil {
		return
	}
	c.stop = make(chan struct{})
	c.done = make(chan struct{})
	stop := c.stop
	done := c.done
	go func() {
		ticker := time.NewTicker(c.setting.CleanupInterval)
		defer ticker.Stop()
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				c.cleanupExpiredTokens()
			}
		}
	}()
}

func (c *redisConnect) stopCleaner() {
	c.mu.Lock()
	if c.stop == nil {
		c.mu.Unlock()
		return
	}
	stop := c.stop
	done := c.done
	c.stop = nil
	c.done = nil
	c.mu.Unlock()
	close(stop)
	<-done
}

func (c *redisConnect) stateError() error {
	if c.opened && c.client != nil {
		return nil
	}
	if c.closed {
		return errClosed
	}
	return mutex.ErrNotReady
}

func normalizeError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return errTimeout
	}
	return err
}

func configDuration(inst *mutex.Instance, field string) time.Duration {
	if inst == nil {
		return 0
	}
	rv := reflect.ValueOf(inst.Config)
	if !rv.IsValid() {
		return 0
	}
	fv := rv.FieldByName(field)
	if !fv.IsValid() || !fv.CanInterface() {
		return 0
	}
	if dur, ok := fv.Interface().(time.Duration); ok {
		return dur
	}
	return 0
}

func randToken() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

func parseDuration(value any) (time.Duration, bool) {
	switch vv := value.(type) {
	case time.Duration:
		return vv, true
	case int:
		return time.Duration(vv) * time.Second, true
	case int64:
		return time.Duration(vv) * time.Second, true
	case float64:
		return time.Duration(vv * float64(time.Second)), true
	case string:
		text := strings.TrimSpace(vv)
		if text == "" {
			return 0, false
		}
		if d, err := time.ParseDuration(text); err == nil {
			return d, true
		}
		if n, err := strconv.ParseFloat(text, 64); err == nil {
			return time.Duration(n * float64(time.Second)), true
		}
		return 0, false
	default:
		return 0, false
	}
}
