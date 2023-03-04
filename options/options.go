// Created by LiuSainan on 2023-03-03 23:01:31

package options

// RedisToFileOptions 用来接收命令行参数
type Options struct {
	SourceFile              string
	SourceHost              string
	SourcePort              int
	SourceUsername          string
	SourcePassword          string
	OutFile                 string
	DestHost                string
	DestPort                int
	DestUsername            string
	DestPassword            string
	DestIdleTimeout         int
	OnlyRedisCommands       string
	IgnoreRedisCommands     string
	AdditionalRedisCommands string
	ChannelSize             int64
	Mode                    int
	LogFile                 string
}
