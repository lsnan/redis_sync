// Created by LiuSainan on 2023-03-04 15:45:52

package service

import (
	"context"
	"fmt"

	"github.com/gomodule/redigo/redis"
	"github.com/hpcloud/tail"
	"github.com/lsnan/redis_sync/options"
)

type Input interface {
	ReadData(ctx context.Context, sch chan<- string) error
	Close() error
}

type SourceRedis struct {
	conn redis.Conn
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

func (sr *SourceRedis) ReadData(ctx context.Context, sch chan<- string) error {
	if _, err := sr.conn.Do("MONITOR"); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("关闭 源端读 redis 线程 ...")
		default:
			if line, err := redis.String(sr.conn.Receive()); err != nil {
				return err
			} else {
				sch <- line
			}
		}
	}
}

func (sr *SourceRedis) Close() error {
	return sr.conn.Close()
}

type SourceFile struct {
	file *tail.Tail
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

func (sf *SourceFile) ReadData(ctx context.Context, sch chan<- string) error {
	for {

		select {
		case <-ctx.Done():
			return fmt.Errorf("关闭 源端读 file 线程 ...")
		default:
			line, ok := <-sf.file.Lines
			if !ok {
				return fmt.Errorf("读文件失败")
			}
			if line.Err != nil {
				return line.Err
			}
			sch <- line.Text
		}
	}
}

func (sf *SourceFile) Close() error {
	return nil
}
