package mutex_redis

import (
	"errors"
	"fmt"
	"sync"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/log"
	"github.com/infrago/mutex"
	"github.com/infrago/util"

	"github.com/gomodule/redigo/redis"
)

type (
	redisDriver  struct{}
	redisConnect struct {
		mutex   sync.RWMutex
		actives int64

		instance *mutex.Instance
		setting  redisSetting

		client *redis.Pool
	}
	redisSetting struct {
		Server   string //服务器地址，ip:端口
		Password string //服务器auth密码
		Database string //数据库

		Idle    int //最大空闲连接
		Active  int //最大激活连接，同时最大并发
		Timeout time.Duration
	}

	redisValue struct {
		Value Any `json:"value"`
	}
)

// 连接
func (driver *redisDriver) Connect(inst *mutex.Instance) (mutex.Connect, error) {
	//获取配置信息
	setting := redisSetting{
		Server: "127.0.0.1:6379", Password: "", Database: "",
		Idle: 30, Active: 100, Timeout: 240,
	}

	if vv, ok := inst.Config.Setting["server"].(string); ok && vv != "" {
		setting.Server = vv
	}
	if vv, ok := inst.Config.Setting["password"].(string); ok && vv != "" {
		setting.Password = vv
	}

	//数据库，redis的0-16号
	if v, ok := inst.Config.Setting["database"].(string); ok {
		setting.Database = v
	}

	if vv, ok := inst.Config.Setting["idle"].(int64); ok && vv > 0 {
		setting.Idle = int(vv)
	}
	if vv, ok := inst.Config.Setting["active"].(int64); ok && vv > 0 {
		setting.Active = int(vv)
	}
	if vv, ok := inst.Config.Setting["timeout"].(int64); ok && vv > 0 {
		setting.Timeout = time.Second * time.Duration(vv)
	}
	if vv, ok := inst.Config.Setting["timeout"].(string); ok && vv != "" {
		td, err := util.ParseDuration(vv)
		if err == nil {
			setting.Timeout = td
		}
	}

	return &redisConnect{
		instance: inst, setting: setting,
	}, nil
}

// 打开连接
func (connect *redisConnect) Open() error {
	connect.client = &redis.Pool{
		MaxIdle: connect.setting.Idle, MaxActive: connect.setting.Active, IdleTimeout: connect.setting.Timeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", connect.setting.Server)
			if err != nil {
				log.Warning("session.redis.dial", err)
				return nil, err
			}

			//如果有验证
			if connect.setting.Password != "" {
				if _, err := c.Do("AUTH", connect.setting.Password); err != nil {
					c.Close()
					log.Warning("session.redis.auth", err)
					return nil, err
				}
			}
			//如果指定库
			if connect.setting.Database != "" {
				if _, err := c.Do("SELECT", connect.setting.Database); err != nil {
					c.Close()
					log.Warning("session.redis.select", err)
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	//打开一个试一下
	conn := connect.client.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	return nil
}

// 关闭连接
func (connect *redisConnect) Close() error {
	if connect.client != nil {
		if err := connect.client.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (connect *redisConnect) Lock(key string, expiry time.Duration) error {
	if connect.client == nil {
		return errors.New("连接失败")
	}
	conn := connect.client.Get()
	defer conn.Close()

	value := fmt.Sprintf("%d", time.Now().UnixNano())

	if expiry <= 0 {
		expiry = connect.instance.Config.Expiry
	}

	args := []Any{
		key, value, "NX", "PX", expiry.Milliseconds(),
	}

	res, err := redis.String(conn.Do("SET", args...))
	if err != nil && err != redis.ErrNil {
		return err
	}
	if res != "OK" {
		//return OK 才是成功
		return errors.New("existed")
	}

	return nil
}
func (connect *redisConnect) Unlock(key string) error {
	if connect.client == nil {
		return errors.New("连接失败")
	}
	conn := connect.client.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	if err != nil {
		return err
	}
	return nil
}

//-------------------- redisBase end -------------------------
