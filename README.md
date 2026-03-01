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

## 说明

- `setting` 仅对当前驱动生效，不同驱动键名可能不同
- 连接失败时优先核对 `setting` 中 host/port/认证/超时等参数
