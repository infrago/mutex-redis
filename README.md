# mutex-redis

`mutex-redis` 是 `mutex` 模块的 `redis` 驱动。

## 安装

```bash
go get github.com/infrago/mutex@latest
go get github.com/infrago/mutex-redis@latest
```

## 接入

```go
import (
    _ "github.com/infrago/mutex"
    _ "github.com/infrago/mutex-redis"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[mutex]
driver = "redis"
```

## 公开 API（摘自源码）

- `func (d *redisDriver) Connect(inst *mutex.Instance) (mutex.Connection, error)`
- `func (c *redisConnect) Open() error`
- `func (c *redisConnect) Close() error`
- `func (c *redisConnect) Lock(key string, expire time.Duration) error`
- `func (c *redisConnect) Unlock(key string) error`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
