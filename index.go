package mutex_redis

import (
	"github.com/infrago/infra"
	"github.com/infrago/mutex"
)

func Driver() mutex.Driver {
	return &redisDriver{}
}

func init() {
	infra.Register("redis", Driver())
}
