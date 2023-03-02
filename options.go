// Created by LiuSainan on 2023-02-27 23:15:05

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

func usage() {
	_, _ = fmt.Fprintf(os.Stderr, `%s : 
Usage: redis_rsync -outfile=redis.log

Options:
`, os.Args[0])

	flag.PrintDefaults()

	_, _ = fmt.Fprintf(os.Stderr, `
  示例 : 

    将本机 redis 6379 实例的 monitor 输出到 redis.log 文件:
	  %s -outfile=redis.log

    将远程 redis 6380 实例的 monitor 输出到 redis.log 文件:
	  %s --source-host='10.10.10.10' -source-port=6380 -source-password='xxxxxxxxxx' --output='file' -outfile=redis.log

    将远程 redis 6380 实例的 monitor 同步到远程 redis 6381:
	  %s --source-host='10.10.10.10' -source-port=6380 -source-password='xxxxxxxxxx' --output='redis' --dest-host='11.11.11.11' --dest-port=6381 --dest-password='xxxxxxxx'

    将远程 redis 6380 实例的 monitor 同步到远程 redis 6381, 并且写入本地文件:
	  %s --source-host='10.10.10.10' -source-port=6380 -source-password='xxxxxxxxxx' --output='both' --dest-host='11.11.11.11' --dest-port=6381 --dest-password='xxxxxxxx' -outfile=redis.log


`, os.Args[0], os.Args[0], os.Args[0], os.Args[0])
}

// 用来接收命令行参数
type Options struct {
	SourceHost     string
	SourcePort     int
	SourceUsername string
	SourcePassword string
	Output         string
	OutFile        string
	LogFile        string
	DestHost       string
	DestPort       int
	DestUsername   string
	DestPassword   string
	// DestMaxIdle             int
	DestIdleTimeout int
	// DestParallel            int
	OnlyRedisCommands       string
	IgnoreRedisCommands     string
	AdditionalRedisCommands string
	ChannelSize             int64
}

func NewOptions() Options {
	var err error
	var option Options
	flag.StringVar(&option.SourceHost, "source-host", "127.0.0.1", "源 redis 实例主机或IP地址")
	flag.IntVar(&option.SourcePort, "source-port", 6379, "源 redis 实列端口号")
	flag.StringVar(&option.SourceUsername, "source-username", "", "源 redis 实例用户名")
	flag.StringVar(&option.SourcePassword, "source-password", "", "源 redis 实例密码")
	flag.StringVar(&option.Output, "output", "file", "输出到哪里, 可选择: <file|redis|both>")
	flag.StringVar(&option.OutFile, "outfile", "", "输出到指定文件的路径")
	flag.StringVar(&option.LogFile, "logfile", "", "日志文件路径")
	flag.StringVar(&option.DestHost, "dest-host", "", "目的 redis 实例主机或IP地址")
	flag.IntVar(&option.DestPort, "dest-port", 6379, "目的 redis 实列端口号")
	flag.StringVar(&option.DestUsername, "dest-username", "", "目的 redis 实例用户名")
	flag.StringVar(&option.DestPassword, "dest-password", "", "目的 redis 实例密码")
	// flag.IntVar(&option.DestMaxIdle, "dest-max-idle", 20, "目的 redis 连接池空闲连接数")
	flag.IntVar(&option.DestIdleTimeout, "dest-idle-timeout", 60, "目的 redis 连接池空闲超时时间(秒)")
	// flag.IntVar(&option.DestParallel, "dest-parallel", 20, "目的 redis 并行写入线程数量")
	flag.StringVar(&option.OnlyRedisCommands, "only-redis-commands", "", "仅仅输出指定的 redis 官方写命令, 以逗号(,)分割, 如: [SET,HSET,RPUSH], 默认输出所有的redis官方写命令")
	flag.StringVar(&option.IgnoreRedisCommands, "ignore-redis-commands", "", "忽略指定的 redis 官方写命令, 以逗号(,)分割, 如: [SET,HSET,RPUSH], 默认输出所有的redis官方写命令")
	flag.StringVar(&option.AdditionalRedisCommands, "additional-redis-commands", "", "输出module等非redis自带的命令, 以逗号(,)分割, 如: [GRAPH.QUERY,JSON.SET], 默认输出所有的redis写命令")
	flag.Int64Var(&option.ChannelSize, "channel-size", 100000, "缓存的命令行数量, 可以缓解对于突发的大流量, 导致 源 redis server 的输出缓冲区膨胀问题")
	flag.Usage = usage
	flag.Parse()

	// if option.DestParallel < 1 {
	// 	option.DestParallel = 1
	// }

	switch option.Output {
	case OutputFile:
		err = option.ValidatorOutputFile()
	case OutputRedis:
		err = option.ValidatorOutputRedis()
	case OutputBoth:
		err = option.ValidatorOutputBoth()
	default:
		err = fmt.Errorf("不支持的输出类型: %s", option.Output)
	}

	if err != nil {
		flag.Usage()
		log.Fatalln(err)
	}

	return option
}

// ValidatorOutputFile 验证输出到文件相关参数
func (o *Options) ValidatorOutputFile() (err error) {
	if o.OutFile == "" {
		return fmt.Errorf("请指定输出文件路径: --outfile")
	}
	return nil
}

// ValidatorOutputRedis 验证输出到 redis 相关参数
func (o *Options) ValidatorOutputRedis() (err error) {
	if o.DestHost == "" {
		return fmt.Errorf("请指定目的 redis 的主机地址: --dest-host")
	}

	if o.SourceHost == o.DestHost && o.SourcePort == o.DestPort {
		return fmt.Errorf("不允许源实例与目的实例的 IP 和 端口 完全相同")
	}
	return nil
}

// ValidatorOutputRedis 验证输出到 redis 相关参数
func (o *Options) ValidatorOutputBoth() (err error) {
	if err := o.ValidatorOutputFile(); err != nil {
		return err
	}
	if err := o.ValidatorOutputRedis(); err != nil {
		return err
	}
	return nil
}
