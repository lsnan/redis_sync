// Created by LiuSainan on 2023-03-03 23:43:57

package options

const (
	RedisToFileMode = iota
	RedisToRedisMode
	RedisToBothMode
	FileToRedisMode
)

var RedisWriteCommands = []string{
	"COPY",
	"DEL",
	"EXPIRE",
	"EXPIREAT",
	"MOVE",
	"PERSIST",
	"PEXPIRE",
	"PEXPIREAT",
	"RENAME",
	"RENAMENX",
	"RESTORE",
	"SORT",
	"TOUCH",
	"UNLINK",
	"APPEND",
	"DECR",
	"DECRBY",
	"GETDEL",
	"GETEX",
	"GETSET",
	"INCR",
	"INCRBY",
	"INCRBYFLOAT",
	"MSET",
	"MSETNX",
	"PSETEX",
	"SET",
	"SETEX",
	"SETNX",
	"SETRANGE",
	"HDEL",
	"HINCRBY",
	"HINCRBYFLOAT",
	"HMSET",
	"HSET",
	"HSETNX",
	"BLMOVE",
	"BLMPOP",
	"BLPOP",
	"BRPOP",
	"BRPOPLPUSH",
	"LINSERT",
	"LMOVE",
	"LMPOP",
	"LPOP",
	"LPUSH",
	"LPUSHX",
	"LREM",
	"LSET",
	"LTRIM",
	"RPOP",
	"RPOPLPUSH",
	"RPUSH",
	"RPUSHX",
	"SADD",
	"SDIFFSTORE",
	"SINTERSTORE",
	"SMOVE",
	"SPOP",
	"SREM",
	"SUNIONSTORE",
	"BZMPOP",
	"BZPOPMAX",
	"BZPOPMIN",
	"ZADD",
	"ZDIFFSTORE",
	"ZINCRBY",
	"ZINTERSTORE",
	"ZMPOP",
	"ZPOPMAX",
	"ZPOPMIN",
	"ZRANGESTORE",
	"ZREM",
	"ZREMRANGEBYLEX",
	"ZREMRANGEBYRANK",
	"ZREMRANGEBYSCORE",
	"ZUNIONSTORE",
}

const Usage = `
功能1: 通过 redis monitor 命令监听一个 redis 实例的写入操作, 并将获取到的操作写入其他 redis 实例, 或者写入到本地文件

功能2: 读取指定文件中记录的redis monitor 命令的输出结果, 将数据写入指定的 redis 实例

示例:

  将 redis 实例的 monitor 输出到 redis_monitor.cmd 文件:
	%s redis-to-file --source-host='10.10.10.10' --source-port=6379 --source-password='xxxxxxxxxx' --outfile=redis_monitor.cmd

  将 redis 实例的 monitor 同步到其他 redis:
	%s redis-to-redis --source-host='10.10.10.10' --source-port=6379 --source-password='xxxxxxxxxx' --dest-host='11.11.11.11' --dest-port=6379 --dest-password='xxxxxxxx'

  将 redis_monitor.cmd 文件中的内容写入到 redis:
	%s file-to-redis --source-file='redis_monitor.cmd' --dest-host='11.11.11.11' --dest-port=6379 --dest-password='xxxxxxxx'
`

// const Usage2 = `
// 功能1: 通过 redis monitor 命令监听一个 redis 实例的写入操作, 并将获取到的操作写入其他 redis 实例, 或者写入到本地文件

// 功能2: 读取指定文件中记录的redis monitor 命令的输出结果, 将数据写入指定的 redis 实例

// 示例:

//   将 redis 实例的 monitor 输出到 redis_monitor.cmd 文件:
// 	%s redis-to-file --source-host='10.10.10.10' --source-port=6379 --source-password='xxxxxxxxxx' --outfile=redis_monitor.cmd

//   将 redis 实例的 monitor 同步到其他 redis:
// 	%s redis-to-redis --source-host='10.10.10.10' --source-port=6379 --source-password='xxxxxxxxxx' --dest-host='11.11.11.11' --dest-port=6379 --dest-password='xxxxxxxx'

//   将 redis 实例的 monitor 同步到其他 redis, 同时也写入到本地文件:
// 	%s redis-to-both --source-host='10.10.10.10' --source-port=6379 --source-password='xxxxxxxxxx' --dest-host='11.11.11.11' --dest-port=6379 --dest-password='xxxxxxxx' --outfile=redis_monitor.cmd

//   将 redis_monitor.cmd 文件中的内容写入到 redis:
// 	%s file-to-redis --source-file='redis_monitor.cmd' --dest-host='11.11.11.11' --dest-port=6379 --dest-password='xxxxxxxx'
// `
