// Created by LiuSainan on 2023-03-04 15:45:52

package service

import (
	"context"
	"fmt"
	"log"

	"github.com/gomodule/redigo/redis"
	"github.com/hpcloud/tail"
	"github.com/lsnan/redis_sync/options"
)

type Input interface {
	ReadData(ctx context.Context, crash chan struct{}, sch chan<- string)
	Close() error
}

type SourceRedis struct {
	conn   redis.Conn
}

func NewSourceRedis(opt options.Options) (Input, error) {
	conn, err := redis.Dial("tcp",
		fmt.Sprintf("%s:%d", opt.SourceHost, opt.SourcePort),
		redis.DialUsername(opt.SourceUsername),
		redis.DialPassword(opt.SourcePassword))
	if err != nil {
		return nil, err
	}
	_, err = conn.Do("PING")
	return &SourceRedis{conn: conn}, err
}

func (sr *SourceRedis) ReadData(ctx context.Context, crash chan struct{}, sch chan<- string) {
	if _, err := sr.conn.Do("MONITOR"); err != nil {
		crash <- struct{}{}
		return
	}

	go func() {
		for {
			if line, err := redis.String(sr.conn.Receive()); err != nil {
				log.Println(err)
				crash <- struct{}{}
				return
			} else {
				sch <- line
			}
		}
	}()

	<-ctx.Done()
	log.Println("关闭 源端读 redis 线程 ...")
	return
}

func (sr *SourceRedis) Close() error {
	return sr.conn.Close()
}

type SourceFile struct {
	file   *tail.Tail
}

func NewSourceFile(opt options.Options) (Input, error) {
	file, err := tail.TailFile(opt.SourceFile, tail.Config{
		ReOpen:    false, //不重新打开
		Follow:    true,  //跟随 tail -f
		MustExist: true,  //文件不存在报错
		Poll:      false,
	})
	return &SourceFile{file: file}, err
}

func (sf *SourceFile) ReadData(ctx context.Context, crash chan struct{}, sch chan<- string) {
	for {

		select {
		case <-ctx.Done():
			log.Println("关闭 源端读 file 线程 ...")
			return
		case line, ok := <-sf.file.Lines:
			if !ok {
				log.Println("读文件失败", line.Err)
				crash <- struct{}{}
				return
			}
			if line.Err != nil {
				log.Println(line.Err)
				crash <- struct{}{}
				return
			}
			sch <- line.Text
		}
	}
}

func (sf *SourceFile) Close() error {
	return nil
}
