// Created by LiuSainan on 2023-03-04 15:45:56

package service

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/lsnan/redis_sync/options"
)

type Output interface {
	WriteData(ctx context.Context, crash chan struct{})
}

type DestRedis struct {
	pool   *redis.Pool
	DestCh chan *RedisMonitorLine
}

func NewDestRedis(opt options.Options, DestCh chan *RedisMonitorLine) (*DestRedis, error) {
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
	return &DestRedis{pool: pool, DestCh: DestCh}, err
}

func (dr *DestRedis) GetConnOfDB(db string) (redis.Conn, error) {
	conn := dr.pool.Get()
	_, err := conn.Do("SELECT", db)
	return conn, err
}

func (dr *DestRedis) WriteData(ctx context.Context, crash chan struct{}) {
	// 写入操作一直使用一个连接, 遇到报错再重新获取, 这样不用每个操作都 SELECT db , 只有遇到 db 改变才需要执行一下
	db := "0"
	conn, err := dr.GetConnOfDB(db)
	if err != nil {
		log.Println("关闭 写目的端 redis 线程 ...")
		crash <- struct{}{}
		return
	}
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	for {
		select {
		case out := <-dr.DestCh:
			i := 0
			for i = 0; i < 3; i++ {
				if conn == nil || err != nil {
					time.Sleep(100 * time.Millisecond)
					if conn != nil {
						conn.Close()
					}
					db = out.DB
					conn, err = dr.GetConnOfDB(db)
					continue
				}

				if db != out.DB {
					db = out.DB
					if _, err = conn.Do("SELECT", db); err != nil {
						time.Sleep(100 * time.Millisecond)
						if conn != nil {
							conn.Close()
						}
						conn, err = dr.GetConnOfDB(db)
						continue
					}
				}

				if _, err = conn.Do(out.Cmd, out.Args...); err != nil {
					time.Sleep(100 * time.Millisecond)
					if conn != nil {
						conn.Close()
					}
					conn, err = dr.GetConnOfDB(db)
					continue
				}
				break
			}
			if i == 3 {
				log.Printf("REDIS WRITE ERROR: DB: %s, CMD: %s, ARGS: %s, ERR: %v\n", out.DB, out.Cmd, out.Args, err)
				crash <- struct{}{}
				return
			}
		case <-ctx.Done():
			log.Println("关闭 写目的端 redis 线程 ...")
			return
		}
	}
}

func (dr *DestRedis) Close() error {
	return dr.pool.Close()
}

type DestFile struct {
	OutFile *os.File
	DestCh  chan string
}

func NewDestFile(opt options.Options, DestCh chan string) (*DestFile, error) {
	OutFile, err := os.OpenFile(opt.OutFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	return &DestFile{OutFile: OutFile, DestCh: DestCh}, err
}

func (df *DestFile) WriteData(ctx context.Context, crash chan struct{}) {
	outputWriter := bufio.NewWriter(df.OutFile)
	defer outputWriter.Flush()

	for {
		select {
		case line := <-df.DestCh:
			if _, err := outputWriter.WriteString(line + "\n"); err != nil {
				log.Println("文件写入失败: ", err)
				crash <- struct{}{}
				return
			}
		case <-ctx.Done():
			log.Println("关闭目的端 写文件 线程 ...")
			return
		case <-time.After(1 * time.Second): //超过1秒没有从 rss.SourceCh 中获取到新数据, 就刷新一次磁盘
			if err := outputWriter.Flush(); err != nil {
				log.Println("文件刷盘失败: ", err)
				crash <- struct{}{}
				return
			}
		}
	}
}

func (df *DestFile) Close() error {
	return df.Close()
}

type DestBoth struct {
	DestFile  *DestFile
	DestRedis *DestRedis
}

func NewDestBoth(DestFile *DestFile, DestRedis *DestRedis) (*DestBoth, error) {
	return &DestBoth{DestFile: DestFile, DestRedis: DestRedis}, nil
}

func (db *DestBoth) WriteData(ctx context.Context, crash chan struct{}) {
	go db.DestFile.WriteData(ctx, crash)
	go db.DestRedis.WriteData(ctx, crash)
}

func (db *DestBoth) Close() error {
	err1 := db.DestFile.Close()
	err2 := db.DestRedis.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
