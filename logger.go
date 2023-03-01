// Created by LiuSainan on 2023-02-28 00:31:17

package main

import (
	"fmt"
	"log"
	"os"
)

// Logger 写文件
type Logger struct {
	LogFile *os.File
	*log.Logger
}

func NewLogger(filename string, prefix string, fileFlag int, logFlag int) (*Logger, error) {
	if filename == "" {
		return &Logger{Logger: log.New(os.Stderr, prefix, logFlag)}, nil
	}

	file, err := os.OpenFile(filename, fileFlag, 0666)
	if err != nil {
		log.Printf("打开文件: %s 失败！", filename)
		return &Logger{Logger: log.New(os.Stderr, prefix, logFlag)}, fmt.Errorf("打开日志文件: %s 失败！", filename)
	}

	return &Logger{LogFile: file, Logger: log.New(file, prefix, logFlag)}, nil
}

// Close 关闭
func (l *Logger) Close() {
	if l.LogFile != nil {
		l.LogFile.Close()
	}
}
