package mutex_redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	base "github.com/infrago/base"
	"github.com/infrago/mutex"
)

func TestRedisUnlockDoesNotDeleteOtherOwnerLock(t *testing.T) {
	server := miniredis.RunT(t)

	inst1 := &mutex.Instance{
		Config: mutex.Config{
			Expire: 50 * time.Millisecond,
			Setting: base.Map{
				"addr": server.Addr(),
			},
		},
	}
	inst2 := &mutex.Instance{
		Config: mutex.Config{
			Expire: time.Second,
			Setting: base.Map{
				"addr": server.Addr(),
			},
		},
	}

	conn1Raw, err := (&redisDriver{}).Connect(inst1)
	if err != nil {
		t.Fatalf("connect1: %v", err)
	}
	conn2Raw, err := (&redisDriver{}).Connect(inst2)
	if err != nil {
		t.Fatalf("connect2: %v", err)
	}
	conn1 := conn1Raw.(*redisConnect)
	conn2 := conn2Raw.(*redisConnect)
	if err := conn1.Open(); err != nil {
		t.Fatalf("open1: %v", err)
	}
	defer conn1.Close()
	if err := conn2.Open(); err != nil {
		t.Fatalf("open2: %v", err)
	}
	defer conn2.Close()

	if err := conn1.Lock("job:1", 50*time.Millisecond); err != nil {
		t.Fatalf("lock1: %v", err)
	}
	server.FastForward(80 * time.Millisecond)
	if err := conn2.Lock("job:1", time.Second); err != nil {
		t.Fatalf("lock2: %v", err)
	}
	if err := conn1.Unlock("job:1"); err != nil {
		t.Fatalf("unlock1: %v", err)
	}
	locked, err := conn2.Locked("job:1")
	if err != nil {
		t.Fatalf("locked: %v", err)
	}
	if !locked {
		t.Fatal("stale unlock removed the new owner's lock")
	}
}

func TestRedisStaleTokenDoesNotUnlockNewOwnerOnSameConnection(t *testing.T) {
	server := miniredis.RunT(t)

	inst := &mutex.Instance{
		Config: mutex.Config{
			Expire: 50 * time.Millisecond,
			Setting: base.Map{
				"addr": server.Addr(),
			},
		},
	}

	connRaw, err := (&redisDriver{}).Connect(inst)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	conn := connRaw.(*redisConnect)
	if err := conn.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	token1, err := conn.LockToken("job:self", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("lock1: %v", err)
	}
	server.FastForward(80 * time.Millisecond)
	token2, err := conn.LockToken("job:self", time.Second)
	if err != nil {
		t.Fatalf("lock2: %v", err)
	}
	if token1 == token2 {
		t.Fatal("expected different lock tokens")
	}
	if err := conn.UnlockToken("job:self", token1); err != nil {
		t.Fatalf("unlock1: %v", err)
	}
	locked, err := conn.Locked("job:self")
	if err != nil {
		t.Fatalf("locked: %v", err)
	}
	if !locked {
		t.Fatal("stale token removed new owner's lock")
	}
}

func TestRedisStaleHelperUnlockDoesNotUnlockNewOwnerOnSameConnection(t *testing.T) {
	server := miniredis.RunT(t)

	inst := &mutex.Instance{
		Config: mutex.Config{
			Expire: 50 * time.Millisecond,
			Setting: base.Map{
				"addr": server.Addr(),
			},
		},
	}

	connRaw, err := (&redisDriver{}).Connect(inst)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	conn := connRaw.(*redisConnect)
	if err := conn.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	if _, err := conn.LockToken("job:helper", 50*time.Millisecond); err != nil {
		t.Fatalf("lock1: %v", err)
	}
	server.FastForward(80 * time.Millisecond)
	token2, err := conn.LockToken("job:helper", time.Second)
	if err != nil {
		t.Fatalf("lock2: %v", err)
	}
	if err := conn.UnlockToken("job:helper", "not-current"); err != nil {
		t.Fatalf("unlock helper old: %v", err)
	}
	locked, err := conn.Locked("job:helper")
	if err != nil {
		t.Fatalf("locked: %v", err)
	}
	if !locked {
		t.Fatal("mismatched token unlock removed current lock")
	}
	if err := conn.UnlockToken("job:helper", token2); err != nil {
		t.Fatalf("unlock2: %v", err)
	}
}

func TestRedisLockedCheckIsReadOnly(t *testing.T) {
	server := miniredis.RunT(t)

	inst := &mutex.Instance{
		Config: mutex.Config{
			Expire: time.Second,
			Setting: base.Map{
				"addr": server.Addr(),
			},
		},
	}

	connRaw, err := (&redisDriver{}).Connect(inst)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	conn := connRaw.(*redisConnect)
	if err := conn.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	if err := conn.Lock("job:2", time.Second); err != nil {
		t.Fatalf("lock: %v", err)
	}
	locked, err := conn.Locked("job:2")
	if err != nil {
		t.Fatalf("locked: %v", err)
	}
	if !locked {
		t.Fatal("expected locked to be true")
	}
	if err := conn.Lock("job:2", time.Second); !errors.Is(err, errLocked) {
		t.Fatalf("expected ErrLocked, got %v", err)
	}
}

func TestRedisRefreshExtendsLease(t *testing.T) {
	server := miniredis.RunT(t)

	inst := &mutex.Instance{
		Config: mutex.Config{
			Expire: 40 * time.Millisecond,
			Setting: base.Map{
				"addr": server.Addr(),
			},
		},
	}

	connRaw, err := (&redisDriver{}).Connect(inst)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	conn := connRaw.(*redisConnect)
	if err := conn.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	token, err := conn.LockToken("job:refresh", 40*time.Millisecond)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	server.FastForward(25 * time.Millisecond)
	if err := conn.RefreshToken("job:refresh", token, 90*time.Millisecond); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	server.FastForward(30 * time.Millisecond)
	if err := conn.Lock("job:refresh", time.Second); !errors.Is(err, errLocked) {
		t.Fatalf("expected lock to remain held, got %v", err)
	}
}

func TestRedisCleanupExpiredTokens(t *testing.T) {
	server := miniredis.RunT(t)

	inst := &mutex.Instance{
		Config: mutex.Config{
			Expire: 30 * time.Millisecond,
			Setting: base.Map{
				"addr": server.Addr(),
			},
		},
	}

	connRaw, err := (&redisDriver{}).Connect(inst)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	conn := connRaw.(*redisConnect)
	if err := conn.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	conn.tokens["job:cleanup"] = tokenState{
		token: "expired",
		until: time.Now().Add(-time.Second),
	}
	if removed := conn.cleanupExpiredTokens(); removed == 0 {
		t.Fatal("expected expired tokens cleanup")
	}
	if len(conn.tokens) != 0 {
		t.Fatalf("expected empty token cache, got %+v", conn.tokens)
	}
}

func TestRedisInvalidLeaseAndClosed(t *testing.T) {
	server := miniredis.RunT(t)

	inst := &mutex.Instance{
		Config: mutex.Config{
			Expire: time.Second,
			Setting: base.Map{
				"addr":             server.Addr(),
				"cleanup_interval": "5s",
			},
		},
	}

	connRaw, err := (&redisDriver{}).Connect(inst)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	conn := connRaw.(*redisConnect)
	if conn.setting.CleanupInterval != 5*time.Second {
		t.Fatalf("unexpected cleanup interval: %s", conn.setting.CleanupInterval)
	}
	if err := conn.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := conn.LockToken("job:invalid", -time.Second); !errors.Is(err, errInvalidLease) {
		t.Fatalf("expected invalid lease, got %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := conn.LockToken("job:closed", time.Second); !errors.Is(err, errClosed) {
		t.Fatalf("expected closed error, got %v", err)
	}
}

func TestRedisNormalizeTimeout(t *testing.T) {
	if !errors.Is(normalizeError(context.DeadlineExceeded), errTimeout) {
		t.Fatalf("expected timeout error mapping")
	}
}
