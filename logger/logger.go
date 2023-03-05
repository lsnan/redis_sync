// Created by LiuSainan on 2023-03-04 15:24:38

package logger

import (
	"log"
	"os"
)

// Logger 写文件
type Logger struct {
	LogFile *os.File
	*log.Logger
}

func NewLogger(filename string, prefix string, fileFlag int, logFlag int) *Logger {
	if filename == "" {
		return &Logger{Logger: log.New(os.Stderr, prefix, logFlag)}
	}

	file, err := os.OpenFile(filename, fileFlag, os.ModePerm)
	if err != nil {
		log.Printf("打开文件: %s 失败！", filename)
		os.Exit(1)
	}

	multiWriter := io.MultiWriter(os.Stdout, file)

	return &Logger{LogFile: file, Logger: log.New(multiWriter, prefix, logFlag)}
}

// Close 关闭
func (l *Logger) Close() {
	if l.LogFile != nil {
		l.LogFile.Close()
	}
}
