// Created by LiuSainan on 2023-02-28 00:51:48

package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

// RedisSyncService 任务
type RedisSyncService struct {
	option        Options
	Crash         chan struct{}
	RedisCommands map[string]struct{}
	SourceConn    redis.Conn
	DestConn      *redis.Pool
	OutFile       *Logger
	Logger        *Logger
	SourceCh      chan string
	OutCh         chan *RedisMonitorLine
	DestCh        chan *RedisMonitorLine
}

func NewRedisSyncService(opt Options, logger *Logger, crash chan struct{}) *RedisSyncService {

	rss := &RedisSyncService{option: opt,
		Crash:    crash,
		Logger:   logger,
		SourceCh: make(chan string, opt.ChannelSize),
		OutCh:    make(chan *RedisMonitorLine, opt.ChannelSize),
		DestCh:   make(chan *RedisMonitorLine, opt.ChannelSize)}

	if err := rss.GetRedisCommands(); err != nil {
		logger.Fatalln(err)
	}

	if err := rss.GetSourceConn(); err != nil {
		logger.Fatalln(err)
	}

	if opt.Output == OutputRedis || opt.Output == OutputBoth {
		if err := rss.GetDestConn(); err != nil {
			logger.Fatalln(err)
		}
	}

	if opt.Output == OutputFile || opt.Output == OutputBoth {
		if err := rss.GetOutter(); err != nil {
			logger.Fatalln(err)
		}
	}

	return rss
}

func (rss *RedisSyncService) GetRedisCommands() (err error) {
	rss.Logger.Println("初始化要监听的 redis 写命令列表")

	var commands = make(map[string]struct{})
	rss.RedisCommands = make(map[string]struct{})

	for _, cmd := range RedisWriteCommands {
		commands[strings.ToUpper(cmd)] = struct{}{}
	}

	// 生成要监听的命令列表, 默认为所有redis官方提供的写命令; 如果手动指定了 OnlyRedisCommands , 会判断 OnlyRedisCommands 列表里是否存在非官方提供的写命令
	if rss.option.OnlyRedisCommands == "" {
		rss.RedisCommands = commands
	} else {
		onlyCmds := strings.Split(rss.option.OnlyRedisCommands, ",")
		for _, cmd := range onlyCmds {
			cmdUpper := strings.ToUpper(strings.Trim(cmd, " "))
			if _, ok := commands[cmdUpper]; ok {
				rss.RedisCommands[cmdUpper] = struct{}{}
			} else {
				return fmt.Errorf("无法识别的 redis 写命令: %s", cmd)
			}
		}
	}

	// 删除忽略的官方命令
	if rss.option.IgnoreRedisCommands != "" {
		ignoreCmds := strings.Split(rss.option.IgnoreRedisCommands, ",")
		for _, cmd := range ignoreCmds {
			cmdUpper := strings.ToUpper(strings.Trim(cmd, " "))
			if _, ok := commands[cmdUpper]; ok {
				delete(rss.RedisCommands, cmdUpper)
			} else {
				return fmt.Errorf("无法识别的 redis 写命令: %s", cmd)
			}
		}
	}

	// 额外的命令不会检查命令合法性
	if rss.option.AdditionalRedisCommands != "" {
		addCmds := strings.Split(rss.option.AdditionalRedisCommands, ",")
		for _, cmd := range addCmds {
			cmdUpper := strings.ToUpper(strings.Trim(cmd, " "))
			rss.RedisCommands[cmdUpper] = struct{}{}
		}
	}

	if len(rss.RedisCommands) == 0 {
		return fmt.Errorf("要监听的 redis 写命令列表为空, 取消任务")
	}

	return nil
}

func (rss *RedisSyncService) PrintRedisCommands() {
	var cmds []string
	for cmd := range rss.RedisCommands {
		cmds = append(cmds, cmd)
	}
	rss.Logger.Println("监听的命令列表:", cmds)
}

func (rss *RedisSyncService) GetSourceConn() (err error) {
	rss.Logger.Println("初始化源库连接")
	if rss.SourceConn, err = redis.Dial("tcp",
		fmt.Sprintf("%s:%d", rss.option.SourceHost, rss.option.SourcePort),
		redis.DialUsername(rss.option.SourceUsername),
		redis.DialPassword(rss.option.SourcePassword)); err != nil {
		return err
	}
	_, err = rss.SourceConn.Do("PING")
	return err
}

func (rss *RedisSyncService) GetDestConn() (err error) {
	// if rss.DestConn, err = redis.Dial("tcp",
	// 	fmt.Sprintf("%s:%d", rss.option.DestHost, rss.option.DestPort),
	// 	redis.DialUsername(rss.option.DestUsername),
	// 	redis.DialPassword(rss.option.DestPassword)); err != nil {
	// 	return err
	// }

	rss.DestConn = &redis.Pool{
		MaxIdle:     rss.option.DestMaxIdle,
		MaxActive:   rss.option.DestParallel + 10,
		IdleTimeout: time.Duration(rss.option.DestIdleTimeout) * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			con, err := redis.Dial("tcp",
				fmt.Sprintf("%s:%d", rss.option.DestHost, rss.option.DestPort),
				redis.DialUsername(rss.option.DestUsername),
				redis.DialPassword(rss.option.DestPassword))
			if err != nil {
				return nil, err
			}
			return con, nil
		},
	}

	conn := rss.DestConn.Get()
	defer conn.Close()

	if conn.Err() != nil {
		return err
	}

	_, err = conn.Do("PING")
	return err
}

func (rss *RedisSyncService) GetOutter() (err error) {
	if rss.OutFile, err = NewLogger(rss.option.OutFile, "", os.O_CREATE|os.O_WRONLY|os.O_APPEND, log.LstdFlags|log.Lmicroseconds); err != nil {
		return err
	}
	return nil
}

func (rss *RedisSyncService) ReadRedisMonitorFormSourceConn() {
	if _, err := rss.SourceConn.Do("MONITOR"); err != nil {
		rss.Logger.Println(err)
		rss.Crash <- struct{}{}
	}
	for {
		if line, err := redis.String(rss.SourceConn.Receive()); err != nil {
			rss.Logger.Println(err)
			rss.Crash <- struct{}{}
		} else {
			rss.SourceCh <- line
		}
	}
}

func (rss *RedisSyncService) HandleMonitorLine() {
	for i := 0; i < rss.option.DestParallel; i++ {
		go func() {
			for line := range rss.SourceCh {
				lineSlices, err := RedisMonitorLineSplit(line)
				if err != nil {
					continue
				}

				if len(lineSlices) < 4 {
					continue
				}

				cmd, err := strconv.Unquote(lineSlices[3])
				if err != nil {
					rss.Logger.Printf("对命令: %s 进行反转义字符串: %s 报错: %v", line, lineSlices[3], err)
					continue
				}
				if _, ok := rss.RedisCommands[strings.ToUpper(cmd)]; !ok {
					continue
				}
				out, err := NewRedisMonitorLine(lineSlices)
				if err != nil {
					rss.Logger.Printf("对命令: %s 进行反转义字符串报错: %v", line, err)
					continue
				}

				if rss.OutFile != nil {
					rss.OutCh <- out
				}
				if rss.DestConn != nil {
					rss.DestCh <- out
				}
			}
		}()
	}

}

func (rss *RedisSyncService) WriteToDestConn() {
	if rss.DestConn == nil {
		return
	}

	for i := 0; i < rss.option.DestParallel; i++ {
		go func() {
			for out := range rss.DestCh {
				conn := rss.DestConn.Get()
				if _, err := conn.Do("SELECT", out.DB); err != nil {
					rss.Logger.Println("REDIS SELECT ERROR:", err, out.Cmd, out.Args)
					break
				}
				if _, err := conn.Do(out.Cmd, out.Args...); err != nil {
					rss.Logger.Println("REDIS WRITE ERROR:", err, out.Cmd, out.Args)
				}
				conn.Close()
			}
		}()
	}
}

func (rss *RedisSyncService) WriteToOutFile() {
	if rss.OutFile == nil {
		return
	}

	go func() {
		for out := range rss.OutCh {
			var line strings.Builder
			if _, err := line.WriteString(out.Timestamp); err != nil {
				continue
			}

			if _, err := line.WriteString(" " + out.DB); err != nil {
				continue
			}

			if _, err := line.WriteString(" " + out.Cmd); err != nil {
				continue
			}

			for _, arg := range out.Args {
				if _, err := line.WriteString(" " + arg.(string)); err != nil {
					continue
				}
			}
			rss.OutFile.Println(line.String())
		}
	}()
}

func (rss *RedisSyncService) Run() {
	rss.PrintRedisCommands()
	rss.WriteToDestConn()
	rss.WriteToOutFile()
	rss.HandleMonitorLine()
	go rss.ReadRedisMonitorFormSourceConn()
}

// ValidatorOutputFile 验证输出到文件相关参数
func (rss *RedisSyncService) Close() {

	if rss.SourceConn != nil {
		rss.Logger.Println("关闭 源 redis 连接...")
		rss.SourceConn.Close()
	}

	if rss.OutCh != nil {
		rss.Logger.Println("关闭 write outfile channel ...")
		close(rss.OutCh)
	}

	if rss.DestCh != nil {
		rss.Logger.Println("关闭 write redis channel ...")
		close(rss.DestCh)
	}

	if rss.DestConn != nil {
		rss.Logger.Println("关闭 目的 redis 连接...")
		rss.DestConn.Close()
	}

	if rss.OutFile != nil {
		rss.Logger.Println("关闭 outfile ...")
		rss.OutFile.Close()
	}
}
