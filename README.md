# mutex-redis

`mutex-redis` 是 `github.com/infrago/mutex` 的**redis 驱动**。

## 包定位

- 类型：驱动
- 作用：把 `mutex` 模块的统一接口落到 `redis` 后端实现

## 快速接入

```go
import (
    _ "github.com/infrago/mutex"
    _ "github.com/infrago/mutex-redis"
)
```

```toml
[mutex]
driver = "redis"
cleanup_interval = "30s"

[mutex.setting]
addr = "127.0.0.1:6379"
timeout = "3s"
```

## `setting` 专用配置项

配置位置：`[mutex].setting`

- `addr`
- `server`
- `username`
- `user`
- `password`
- `pass`
- `database`
- `timeout`
- `cleanup_interval`

## 说明

- `setting` 仅对当前驱动生效，不同驱动键名可能不同
- 连接失败时优先核对 `setting` 中 host/port/认证/超时等参数
- 驱动内部使用随机 token + Lua compare-delete 解锁，过期旧锁不会误删新 owner 的锁
- 驱动内部使用 Lua compare-refresh 续租，只有当前 token owner 才能延长租约
- `Locked()` 走 Redis `EXISTS`，是只读检查
- `timeout` 命中时会尽量归一成 `ErrTimeout`
- 连接关闭后再次使用会返回 `ErrClosed`
- 负数 lease 会返回 `ErrInvalidLease`
- `cleanup_interval` 控制驱动本地 token 状态清理周期，默认 `30s`
- 驱动会在本地缓存 token，并自动清理过期 token 状态，避免长时间运行后本地状态堆积
- 需要区分“锁存在”和“Redis 检查失败”时，建议通过主包使用 `mutex.Check()` / `mutex.CheckOn()`
- 长任务建议通过主包的 `mutex.Refresh()` / `locker.Refresh()` 定期续租
