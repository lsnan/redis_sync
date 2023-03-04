// Created by LiuSainan on 2023-03-04 15:22:45

package service

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lsnan/redis_sync/logger"
	"github.com/lsnan/redis_sync/options"
	"github.com/lsnan/redis_sync/utils"
)

// RedisSyncService 任务
type RedisSyncService struct {
	ctx           context.Context
	option        options.Options
	Crash         chan struct{}
	RedisCommands map[string]struct{}
	Source        Input
	Dest          *DestRedis
	OutFile       *os.File
	Logger        *logger.Logger
	SourceCh      chan string
	DestCh        chan *RedisMonitorLine
}

func NewRedisSyncService(ctx context.Context, opt options.Options, logger *logger.Logger, crash chan struct{}) (*RedisSyncService, error) {
	rss := &RedisSyncService{
		ctx:      ctx,
		option:   opt,
		Crash:    crash,
		Logger:   logger,
		SourceCh: make(chan string, opt.ChannelSize),
		DestCh:   make(chan *RedisMonitorLine, opt.ChannelSize),
	}

	if err := rss.GetRedisCommands(); err != nil {
		log.Fatalln(err)
	}

	if err := rss.GetSourceConn(); err != nil {
		log.Fatalln(err)
	}

	if err := rss.GetDestConn(); err != nil {
		log.Fatalln(err)
	}

	return rss, nil
}

func (rss *RedisSyncService) GetRedisCommands() (err error) {
	rss.Logger.Println("初始化要监听的 redis 写命令列表")

	var commands = make(map[string]struct{})
	rss.RedisCommands = make(map[string]struct{})

	for _, cmd := range options.RedisWriteCommands {
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
	if rss.option.Mode == options.FileToRedisMode {
		rss.Logger.Println("初始化源文件")
		if rss.Source, err = NewSourceFile(rss.option); err != nil {
			log.Fatalln(err)
		}
	} else {
		rss.Logger.Println("初始化源库连接")
		if rss.Source, err = NewSourceRedis(rss.option); err != nil {
			log.Fatalln(err)
		}
	}
	return err
}

func (rss *RedisSyncService) GetDestConn() (err error) {
	if rss.option.Mode == options.RedisToFileMode || rss.option.Mode == options.RedisToBothMode {
		rss.Logger.Println("初始化输出文件")
		if rss.OutFile, err = os.Create(rss.option.OutFile); err != nil {
			log.Fatalln(err)
		}
	} else {
		if rss.Dest, err = NewDestRedis(rss.option); err != nil {
			log.Fatalln(err)
		}
	}
	return err
}

func (rss *RedisSyncService) HandleMonitorLine(ctx context.Context) {
	for {
		select {
		case line := <-rss.SourceCh:
			lineSlices, err := utils.RedisMonitorLineSplit(line)
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
			rss.DestCh <- out
		case <-ctx.Done():
			rss.Logger.Println("关闭 monitor 输出行处理 线程 ...")
			return
		}
	}

	// for line := range rss.SourceCh {
	// 	lineSlices, err := utils.RedisMonitorLineSplit(line)
	// 	if err != nil {
	// 		continue
	// 	}

	// 	if len(lineSlices) < 4 {
	// 		continue
	// 	}

	// 	cmd, err := strconv.Unquote(lineSlices[3])
	// 	if err != nil {
	// 		rss.Logger.Printf("对命令: %s 进行反转义字符串: %s 报错: %v", line, lineSlices[3], err)
	// 		continue
	// 	}
	// 	if _, ok := rss.RedisCommands[strings.ToUpper(cmd)]; !ok {
	// 		continue
	// 	}
	// 	out, err := NewRedisMonitorLine(lineSlices)
	// 	if err != nil {
	// 		rss.Logger.Printf("对命令: %s 进行反转义字符串报错: %v", line, err)
	// 		continue
	// 	}
	// 	rss.DestCh <- out
	// }
}

func (rss *RedisSyncService) ReadData(ctx context.Context) {
	if err := rss.Source.ReadData(ctx, rss.SourceCh); err != nil {
		rss.Logger.Println(err)
		rss.Crash <- struct{}{}
	}
}

func (rss *RedisSyncService) WriteToDestRedis(ctx context.Context) {
	if err := rss.Dest.WriteData(ctx, rss.DestCh); err != nil {
		rss.Logger.Println(err)
		rss.Crash <- struct{}{}
	}
}

func (rss *RedisSyncService) WriteToOutFile(ctx context.Context) {
	outputWriter := bufio.NewWriter(rss.OutFile)
	defer outputWriter.Flush()

	for {
		select {
		case line := <-rss.SourceCh:
			if _, err := outputWriter.WriteString(line + "\n"); err != nil {
				rss.Logger.Println(err)
				rss.Crash <- struct{}{}
				return
			}
		case <-ctx.Done():
			rss.Logger.Println("关闭目的端 写文件 线程 ...")
			return
		case <-time.After(1 * time.Second): //超过1秒没有从 rss.SourceCh 中获取到新数据, 就刷新一次磁盘
			if err := outputWriter.Flush(); err != nil {
				rss.Logger.Println(err)
				rss.Crash <- struct{}{}
				return
			}
		}
	}
}

// Close 关闭
func (rss *RedisSyncService) Close() {

	if rss.Source != nil {
		rss.Logger.Println("关闭 源 redis 连接...")
		rss.Source.Close()
	}

	if rss.SourceCh != nil {
		rss.Logger.Println("关闭 源 redis channel...")
		close(rss.SourceCh)
	}

	if rss.DestCh != nil {
		rss.Logger.Println("关闭 write redis channel ...")
		close(rss.DestCh)
	}

	if rss.Dest != nil {
		rss.Logger.Println("关闭 目的端 redis 连接...")
		rss.Dest.Close()
	}

	if rss.OutFile != nil {
		rss.Logger.Println("关闭 outfile ...")
		rss.OutFile.Close()
	}
}

func (rss *RedisSyncService) Run() {
	rss.PrintRedisCommands()
	if rss.option.Mode == options.RedisToFileMode || rss.option.Mode == options.RedisToBothMode {
		go rss.WriteToOutFile(rss.ctx)
	} else {
		go rss.WriteToDestRedis(rss.ctx)
		go rss.HandleMonitorLine(rss.ctx)
	}
	go rss.ReadData(rss.ctx)
}
