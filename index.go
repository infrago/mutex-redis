package mutex_redis

import (
	"github.com/infrago/mutex"
)

func Driver() mutex.Driver {
	return &redisDriver{}
}

func init() {
	mutex.Register("redis", Driver())
}
