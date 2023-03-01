// Created by LiuSainan on 2023-02-27 21:47:29

package main

import (
	"log"
	"os"
	"os/signal"
)

func main() {
	opt := NewOptions()

	logger, _ := NewLogger(opt.LogFile, "", os.O_CREATE|os.O_WRONLY|os.O_APPEND, log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	defer logger.Close()

	crash := make(chan struct{}, 1)
	rss := NewRedisSyncService(opt, logger, crash)
	defer rss.Close()

	rss.Run()

	// Wait for interrupt signal to gracefully shutdown the server with
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	select {
	case <-quit:
	case <-crash:
	}

	logger.Println("Shutdown Server ...")
}
