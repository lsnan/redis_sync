// Created by LiuSainan on 2023-03-04 15:22:45

package service

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

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
	Dest          Output
	Logger        *logger.Logger
	SourceCh      chan string
	OutFileCh     chan string
	OutRedisCh    chan *RedisMonitorLine
}

func NewRedisSyncService(ctx context.Context, opt options.Options, logger *logger.Logger, crash chan struct{}) (*RedisSyncService, error) {
	rss := &RedisSyncService{
		ctx:        ctx,
		option:     opt,
		Crash:      crash,
		Logger:     logger,
		SourceCh:   make(chan string, opt.ChannelSize),
		OutFileCh:  make(chan string, opt.ChannelSize),
		OutRedisCh: make(chan *RedisMonitorLine, opt.ChannelSize),
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
		if rss.Source, err = NewSourceFile(rss.option, rss.Logger); err != nil {
			return err
		}
	} else {
		rss.Logger.Println("初始化源库连接")
		if rss.Source, err = NewSourceRedis(rss.option, rss.Logger); err != nil {
			return err
		}
	}
	return err
}

func (rss *RedisSyncService) GetDestConn() (err error) {
	if rss.option.Mode == options.RedisToFileMode {
		if rss.Dest, err = NewDestFile(rss.option, rss.OutFileCh, rss.Logger); err != nil {
			return err
		}
	}

	if rss.option.Mode == options.RedisToRedisMode || rss.option.Mode == options.FileToRedisMode {
		if rss.Dest, err = NewDestRedis(rss.option, rss.OutRedisCh, rss.Logger); err != nil {
			return err
		}
	}

	if rss.option.Mode == options.RedisToBothMode {
		Dest1, err := NewDestFile(rss.option, rss.OutFileCh, rss.Logger)
		if err != nil {
			return err
		}

		Dest2, err := NewDestRedis(rss.option, rss.OutRedisCh, rss.Logger)
		if err != nil {
			return err
		}
		if rss.Dest, err = NewDestBoth(Dest1, Dest2); err != nil {
			return err
		}

	}
	return nil
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
			if rss.option.Mode == options.RedisToFileMode || rss.option.Mode == options.RedisToBothMode {
				rss.OutFileCh <- line
				if rss.option.Mode == options.RedisToFileMode {
					continue
				}
			}

			out, err := NewRedisMonitorLine(lineSlices)
			if err != nil {
				rss.Logger.Printf("对命令: %s 进行反转义字符串报错: %v", line, err)
				continue
			}
			rss.OutRedisCh <- out
		case <-ctx.Done():
			rss.Logger.Println("关闭 monitor 输出行处理 线程 ...")
			return
		}
	}
}

// Close 关闭
func (rss *RedisSyncService) Close() {

	if rss.Source != nil {
		rss.Logger.Println("关闭 源端 连接...")
		rss.Source.Close()
	}

	if rss.SourceCh != nil {
		rss.Logger.Println("关闭 源 redis channel...")
		close(rss.SourceCh)
	}

	if rss.OutRedisCh != nil {
		rss.Logger.Println("关闭 write redis channel ...")
		close(rss.OutRedisCh)
	}

	if rss.OutFileCh != nil {
		rss.Logger.Println("关闭 write file channel ...")
		close(rss.OutFileCh)
	}
	if rss.Dest != nil {
		rss.Logger.Println("关闭 目的端 连接...")
		rss.Source.Close()
	}
}

func (rss *RedisSyncService) Run() {
	rss.PrintRedisCommands()
	go rss.Dest.WriteData(rss.ctx, rss.Crash)
	go rss.HandleMonitorLine(rss.ctx)
	go rss.Source.ReadData(rss.ctx, rss.Crash, rss.SourceCh)
}
