// Created by LiuSainan on 2023-03-04 22:21:20

package service

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/lsnan/redis_sync/logger"
	"github.com/lsnan/redis_sync/options"
)

func Server(opt options.Options) {
	logger := logger.NewLogger(opt.LogFile, "", os.O_CREATE|os.O_WRONLY|os.O_APPEND, log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	defer logger.Close()

	ctx, cancel := context.WithCancel(context.Background())
	crash := make(chan struct{}, 1)

	rss, err := NewRedisSyncService(ctx, opt, logger, crash)
	if err != nil {
		log.Fatalln(err)
	}

	rss.Run()

	// Wait for interrupt signal to gracefully shutdown the server with
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	select {
	case <-quit:
		logger.Println("Ctrl + c Shutdown Server ...")
	case <-crash:
		logger.Println("Crash Shutdown Server ...")
	}

	cancel()
	time.Sleep(100 * time.Millisecond)
	rss.Close()
	logger.Println("Server exiting")
}

// func RedisToFile(opt options.Options, logger *logger.Logger, crash chan struct{}) {
// 	if opt.OutFile == "" {
// 		log.Fatalln("请指定输出文件")
// 	}
// 	rss, err := NewRedisSyncService(opt, logger, crash)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	defer rss.Close()

// 	if rss.Source, err = NewSourceRedis(opt); err != nil {
// 		log.Fatalln(err)
// 	}
// 	defer func() error {
// 		return rss.Source.Close()
// 	}()

// 	if rss.OutFile, err = os.Create(opt.OutFile); err != nil {
// 		log.Fatalln(err)
// 	}
// 	defer rss.OutFile.Close()

// 	go rss.WriteToOutFile()
// 	go rss.ReadData()
// }

// func RedisToRedis(opt options.Options, logger *logger.Logger, crash chan struct{}) {
// 	rss, err := NewRedisSyncService(opt, logger, crash)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	defer rss.Close()

// 	rss.DestCh = make(chan *RedisMonitorLine, opt.ChannelSize)

// 	if rss.Source, err = NewSourceRedis(opt); err != nil {
// 		log.Fatalln(err)
// 	}
// 	defer func() error {
// 		return rss.Source.Close()
// 	}()

// 	if rss.Dest, err = NewDestRedis(opt); err != nil {
// 		log.Fatalln(err)
// 	}
// 	defer rss.Dest.Close()

// 	go rss.WriteToDestRedis()
// 	go rss.HandleMonitorLine()
// 	go rss.ReadData()
// }

// func FileToRedis(opt options.Options, logger *logger.Logger, crash chan struct{}) {
// 	rss, err := NewRedisSyncService(opt, logger, crash)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	defer rss.Close()

// 	rss.DestCh = make(chan *RedisMonitorLine, opt.ChannelSize)

// 	if rss.Source, err = NewSourceFile(opt); err != nil {
// 		log.Fatalln(err)
// 	}

// 	if rss.Dest, err = NewDestRedis(opt); err != nil {
// 		log.Fatalln(err)
// 	}
// 	defer rss.Dest.Close()

// 	go rss.WriteToDestRedis()
// 	go rss.HandleMonitorLine()
// 	go rss.ReadData()
// }
