// Created by LiuSainan on 2023-03-04 22:21:20

package service

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/lsnan/redis_sync/options"
)

func Server(opt options.Options) {
	ctx, cancel := context.WithCancel(context.Background())
	crash := make(chan struct{}, 1)

	rss, err := NewRedisSyncService(ctx, opt, crash)
	if err != nil {
		log.Fatalln(err)
	}

	rss.Run()

	// Wait for interrupt signal to gracefully shutdown the server with
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	select {
	case <-quit:
		log.Println("Ctrl + c Shutdown Server ...")
	case <-crash:
		log.Println("Crash Shutdown Server ...")
	}

	cancel()
	time.Sleep(100 * time.Millisecond)
	rss.Close()
	log.Println("Server exiting")
}