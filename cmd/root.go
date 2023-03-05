/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/lsnan/redis_sync/options"
	"github.com/lsnan/redis_sync/service"
	"github.com/spf13/cobra"
)

var _version string
var logFilename string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "redis_sync",
	Short: "",
	Long:  fmt.Sprintf(options.Usage, os.Args[0], os.Args[0], os.Args[0], os.Args[0]),
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func versionCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "version",
		Short: "redis_sync 版本",
		Run: func(cmd *cobra.Command, args []string) {
			if _version == "" {
				_version = time.Now().String()
			}
			fmt.Printf("redis_sync version %s\n", _version)
		},
	}

	return cmd
}

// redis_sync redis-to-file
func RedisToFileCmd() *cobra.Command {
	opt := options.Options{Mode: options.RedisToFileMode, LogFile: logFilename}
	cmd := &cobra.Command{
		Use:   "redis-to-file",
		Short: "将 redis 实例的 monitor 输出到指定的文件",
		Run: func(cmd *cobra.Command, args []string) {
			if opt.OutFile == "" {
				fmt.Println("请指定输出文件...")
			}

			service.Server(opt)
		},
	}
	cmd.Flags().StringVar(&opt.SourceHost, "source-host", "127.0.0.1", "源 redis 实例主机或IP地址")
	cmd.Flags().IntVar(&opt.SourcePort, "source-port", 6379, "源 redis 实列端口号")
	cmd.Flags().StringVar(&opt.SourceUsername, "source-username", "", "源 redis 实例用户名")
	cmd.Flags().StringVar(&opt.SourcePassword, "source-password", "", "源 redis 实例密码")
	cmd.Flags().StringVar(&opt.OutFile, "out-file", "redis-monitor.cmd", "输出到指定文件的文件")
	cmd.Flags().StringVar(&opt.OnlyRedisCommands, "only-redis-commands", "", "仅仅输出指定的 redis 官方写命令, 以逗号(,)分割, 如: [SET,HSET,RPUSH], 默认输出所有的redis官方写命令")
	cmd.Flags().StringVar(&opt.IgnoreRedisCommands, "ignore-redis-commands", "", "忽略指定的 redis 官方写命令, 以逗号(,)分割, 如: [SET,HSET,RPUSH], 默认输出所有的redis官方写命令")
	cmd.Flags().StringVar(&opt.AdditionalRedisCommands, "additional-redis-commands", "", "输出module等非redis自带的命令, 以逗号(,)分割, 如: [GRAPH.QUERY,JSON.SET], 默认输出所有的redis写命令")
	cmd.Flags().Int64Var(&opt.ChannelSize, "channel-size", 100000, "缓存的命令行数量, 可以缓解对于突发的大流量, 导致 源 redis server 的输出缓冲区膨胀问题")

	return cmd
}

// redis_sync redis-to-redis
func RedisToRedisCmd() *cobra.Command {
	opt := options.Options{Mode: options.RedisToRedisMode, LogFile: logFilename}
	cmd := &cobra.Command{
		Use:   "redis-to-redis",
		Short: "将 redis 实例的 monitor 同步到其他 redis",
		Run: func(cmd *cobra.Command, args []string) {
			service.Server(opt)
		},
	}
	cmd.Flags().StringVar(&opt.SourceHost, "source-host", "127.0.0.1", "源 redis 实例主机或IP地址")
	cmd.Flags().IntVar(&opt.SourcePort, "source-port", 6379, "源 redis 实列端口号")
	cmd.Flags().StringVar(&opt.SourceUsername, "source-username", "", "源 redis 实例用户名")
	cmd.Flags().StringVar(&opt.SourcePassword, "source-password", "", "源 redis 实例密码")
	cmd.Flags().StringVar(&opt.DestHost, "dest-host", "", "目的 redis 实例主机或IP地址")
	cmd.Flags().IntVar(&opt.DestPort, "dest-port", 6379, "目的 redis 实列端口号")
	cmd.Flags().StringVar(&opt.DestUsername, "dest-username", "", "目的 redis 实例用户名")
	cmd.Flags().StringVar(&opt.DestPassword, "dest-password", "", "目的 redis 实例密码")
	cmd.Flags().IntVar(&opt.DestIdleTimeout, "dest-idle-timeout", 60, "目的 redis 连接池空闲超时时间(秒)")
	cmd.Flags().StringVar(&opt.OnlyRedisCommands, "only-redis-commands", "", "仅仅输出指定的 redis 官方写命令, 以逗号(,)分割, 如: [SET,HSET,RPUSH], 默认输出所有的redis官方写命令")
	cmd.Flags().StringVar(&opt.IgnoreRedisCommands, "ignore-redis-commands", "", "忽略指定的 redis 官方写命令, 以逗号(,)分割, 如: [SET,HSET,RPUSH], 默认输出所有的redis官方写命令")
	cmd.Flags().StringVar(&opt.AdditionalRedisCommands, "additional-redis-commands", "", "输出module等非redis自带的命令, 以逗号(,)分割, 如: [GRAPH.QUERY,JSON.SET], 默认输出所有的redis写命令")
	cmd.Flags().Int64Var(&opt.ChannelSize, "channel-size", 100000, "缓存的命令行数量, 可以缓解对于突发的大流量, 导致 源 redis server 的输出缓冲区膨胀问题")

	return cmd
}

// redis_sync redis-to-both
func RedisToBothCmd() *cobra.Command {
	opt := options.Options{Mode: options.RedisToBothMode, LogFile: logFilename}
	cmd := &cobra.Command{
		Use:   "redis-to-both",
		Short: "将 redis 实例的 monitor 同步到其他 redis, 同时也写入到本地文件",
		Run: func(cmd *cobra.Command, args []string) {
			service.Server(opt)
		},
	}
	cmd.Flags().StringVar(&opt.SourceHost, "source-host", "127.0.0.1", "源 redis 实例主机或IP地址")
	cmd.Flags().IntVar(&opt.SourcePort, "source-port", 6379, "源 redis 实列端口号")
	cmd.Flags().StringVar(&opt.SourceUsername, "source-username", "", "源 redis 实例用户名")
	cmd.Flags().StringVar(&opt.SourcePassword, "source-password", "", "源 redis 实例密码")
	cmd.Flags().StringVar(&opt.OutFile, "out-file", "", "输出到指定文件的文件")
	cmd.Flags().StringVar(&opt.DestHost, "dest-host", "", "目的 redis 实例主机或IP地址")
	cmd.Flags().IntVar(&opt.DestPort, "dest-port", 6379, "目的 redis 实列端口号")
	cmd.Flags().StringVar(&opt.DestUsername, "dest-username", "", "目的 redis 实例用户名")
	cmd.Flags().StringVar(&opt.DestPassword, "dest-password", "", "目的 redis 实例密码")
	cmd.Flags().IntVar(&opt.DestIdleTimeout, "dest-idle-timeout", 60, "目的 redis 连接池空闲超时时间(秒)")
	cmd.Flags().StringVar(&opt.OnlyRedisCommands, "only-redis-commands", "", "仅仅输出指定的 redis 官方写命令, 以逗号(,)分割, 如: [SET,HSET,RPUSH], 默认输出所有的redis官方写命令")
	cmd.Flags().StringVar(&opt.IgnoreRedisCommands, "ignore-redis-commands", "", "忽略指定的 redis 官方写命令, 以逗号(,)分割, 如: [SET,HSET,RPUSH], 默认输出所有的redis官方写命令")
	cmd.Flags().StringVar(&opt.AdditionalRedisCommands, "additional-redis-commands", "", "输出module等非redis自带的命令, 以逗号(,)分割, 如: [GRAPH.QUERY,JSON.SET], 默认输出所有的redis写命令")
	cmd.Flags().Int64Var(&opt.ChannelSize, "channel-size", 100000, "缓存的命令行数量, 可以缓解对于突发的大流量, 导致 源 redis server 的输出缓冲区膨胀问题")

	return cmd
}

// redis_sync file-to-redis
func FileToRedisCmd() *cobra.Command {
	opt := options.Options{Mode: options.FileToRedisMode, LogFile: logFilename}
	cmd := &cobra.Command{
		Use:   "file-to-redis",
		Short: "将指定文件中的内容写入到 redis",
		Run: func(cmd *cobra.Command, args []string) {
			service.Server(opt)
		},
	}
	cmd.Flags().StringVar(&opt.SourceFile, "source-file", "", "要读取的 redis 文件")
	cmd.Flags().StringVar(&opt.DestHost, "dest-host", "", "目的 redis 实例主机或IP地址")
	cmd.Flags().IntVar(&opt.DestPort, "dest-port", 6379, "目的 redis 实列端口号")
	cmd.Flags().StringVar(&opt.DestUsername, "dest-username", "", "目的 redis 实例用户名")
	cmd.Flags().StringVar(&opt.DestPassword, "dest-password", "", "目的 redis 实例密码")
	cmd.Flags().IntVar(&opt.DestIdleTimeout, "dest-idle-timeout", 60, "目的 redis 连接池空闲超时时间(秒)")
	cmd.Flags().StringVar(&opt.OnlyRedisCommands, "only-redis-commands", "", "仅仅输出指定的 redis 官方写命令, 以逗号(,)分割, 如: [SET,HSET,RPUSH], 默认输出所有的redis官方写命令")
	cmd.Flags().StringVar(&opt.IgnoreRedisCommands, "ignore-redis-commands", "", "忽略指定的 redis 官方写命令, 以逗号(,)分割, 如: [SET,HSET,RPUSH], 默认输出所有的redis官方写命令")
	cmd.Flags().StringVar(&opt.AdditionalRedisCommands, "additional-redis-commands", "", "输出module等非redis自带的命令, 以逗号(,)分割, 如: [GRAPH.QUERY,JSON.SET], 默认输出所有的redis写命令")
	cmd.Flags().Int64Var(&opt.ChannelSize, "channel-size", 100000, "缓存的命令行数量, 可以缓解对于突发的大流量, 导致 源 redis server 的输出缓冲区膨胀问题")

	return cmd
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&logFilename, "log", "", "标准输出写入日志文件")
	rootCmd.AddCommand(
		versionCmd(),
		RedisToFileCmd(),
		RedisToRedisCmd(),
		RedisToBothCmd(),
		FileToRedisCmd(),
	)
}
