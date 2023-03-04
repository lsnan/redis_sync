// Created by LiuSainan on 2023-03-04 15:45:56

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/lsnan/redis_sync/options"
)

// 接口内的函数不支持泛型; 使用 chan infance{} 在进行类型断言时又消耗性能; 没想到好办法, 写到 redis 和 写到 file 只能分别搞了
// type Output interface {
// 	WriteData(dch <-chan *RedisMonitorLine) error
// }

type DestRedis struct {
	pool *redis.Pool
}

func NewDestRedis(opt options.Options) (*DestRedis, error) {
	// if rss.DestConn, err = redis.Dial("tcp",
	// 	fmt.Sprintf("%s:%d", rss.option.DestHost, rss.option.DestPort),
	// 	redis.DialUsername(rss.option.DestUsername),
	// 	redis.DialPassword(rss.option.DestPassword)); err != nil {
	// 	return err
	// }

	pool := &redis.Pool{
		// MaxIdle:     rss.option.DestMaxIdle,
		// MaxActive:   rss.option.DestParallel + 10,
		IdleTimeout: time.Duration(opt.DestIdleTimeout) * time.Second,
		MaxIdle:     10,
		MaxActive:   20,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			con, err := redis.Dial("tcp",
				fmt.Sprintf("%s:%d", opt.DestHost, opt.DestPort),
				redis.DialUsername(opt.DestUsername),
				redis.DialPassword(opt.DestPassword),
				redis.DialDatabase(0))
			if err != nil {
				return nil, err
			}
			return con, nil
		},
	}

	conn := pool.Get()
	defer conn.Close()

	if conn.Err() != nil {
		return nil, conn.Err()
	}

	_, err := conn.Do("PING")
	return &DestRedis{pool: pool}, err
}

func (dr *DestRedis) GetConnOfDB(db string) (redis.Conn, error) {
	conn := dr.pool.Get()
	_, err := conn.Do("SELECT", db)
	return conn, err
}

func (dr *DestRedis) WriteData(ctx context.Context, dch <-chan *RedisMonitorLine) error {
	// 写入操作一直使用一个连接, 遇到报错再重新获取, 这样不用每个操作都 SELECT db , 只有遇到 db 改变才需要执行一下
	db := "0"
	conn, err := dr.GetConnOfDB(db)
	if err != nil {
		return err
	}
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	for {
		select {
		case out := <-dch:
			i := 0
			for i = 0; i < 3; i++ {
				if conn == nil || err != nil || db != out.DB {
					time.Sleep(100 * time.Millisecond)
					db = out.DB
					conn, err = dr.GetConnOfDB(db)
					continue
				}

				if _, err = conn.Do(out.Cmd, out.Args...); err != nil {
					time.Sleep(100 * time.Millisecond)
					conn, err = dr.GetConnOfDB(db)
					continue
				}
				break
			}
			if i == 3 {
				return fmt.Errorf("REDIS WRITE ERROR: DB: %s, CMD: %s, ARGS: %s, ERR: %v\n", out.DB, out.Cmd, out.Args, err)
			}
		case <-ctx.Done():
			return fmt.Errorf("关闭 写目的端 redis 线程 ...")
		}
	}
	// for out := range dch {
	// 	i := 0
	// 	for i = 0; i < 3; i++ {
	// 		if conn == nil || err != nil || db != out.DB {
	// 			time.Sleep(100 * time.Millisecond)
	// 			db = out.DB
	// 			conn, err = dr.GetConnOfDB(db)
	// 			continue
	// 		}

	// 		if _, err = conn.Do(out.Cmd, out.Args...); err != nil {
	// 			time.Sleep(100 * time.Millisecond)
	// 			conn, err = dr.GetConnOfDB(db)
	// 			continue
	// 		}
	// 		break
	// 	}
	// 	if i == 3 {
	// 		return fmt.Errorf("REDIS WRITE ERROR: DB: %s, CMD: %s, ARGS: %s, ERR: %v\n", out.DB, out.Cmd, out.Args, err)
	// 	}
	// }
	return nil
}

func (dr *DestRedis) Close() error {
	return dr.pool.Close()
}
