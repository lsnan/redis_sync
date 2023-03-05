# redis_sync

### 通过 redis monitor 命令实现 redis 到 redis 的同步迁移

## 使用方法

1. 下载代码

```
git clone https://github.com/lsnan/redis_sync.git
```

2. 进入目录, 编译

```
cd redis_sync
go mod tidy
go build
```

3. 执行 redis_sync 

```
功能1: 通过 redis monitor 命令监听一个 redis 实例的写入操作, 并将获取到的操作写入其他 redis 实例, 或者写入到本地文件

功能2: 读取指定文件中记录的redis monitor 命令的输出结果, 将数据写入指定的 redis 实例

示例:

  将 redis 实例的 monitor 输出到 redis_monitor.cmd 文件:
	redis_sync redis-to-file --source-host='10.10.10.10' --source-port=6379 --source-password='xxxxxxxxxx' --outfile=redis_monitor.cmd

  将 redis 实例的 monitor 同步到其他 redis:
	redis_sync redis-to-redis --source-host='10.10.10.10' --source-port=6379 --source-password='xxxxxxxxxx' --dest-host='11.11.11.11' --dest-port=6379 --dest-password='xxxxxxxx'

  将 redis 实例的 monitor 同步到其他 redis, 同时也写入到本地文件:
	redis_sync redis-to-both --source-host='10.10.10.10' --source-port=6379 --source-password='xxxxxxxxxx' --dest-host='11.11.11.11' --dest-port=6379 --dest-password='xxxxxxxx' --outfile=redis_monitor.cmd

  将 redis_monitor.cmd 文件中的内容写入到 redis:
	redis_sync file-to-redis --source-file='redis_monitor.cmd' --dest-host='11.11.11.11' --dest-port=6379 --dest-password='xxxxxxxx'
```

## Features

- 将 redis monitor 获取到的相关命令同步在目标 redis 库执行
- 将 redis monitor 获取到的相关命令输出到指定文件
- 将 文件中的 redis monitor 的相关命令同步到目的端 redis 实例
- redis monitor 获取到的相关命令中, 支持 key 和 参数 包含空格, 换行, 转义字符等特殊字符

## 注意事项

- 不同步原始的基础数据, 从运行 redis_sync 命令开始同步 monitor 看到的命令(即只有增量)。
- 源端 redis 连接异常, 程序会直接退出
- 目的端 redis 连接异常或写入异常,程序会直接退出
- 由于是单线程写入, 所以遇到阻塞命令(如 BLPOP) 并且在 目的端执行被阻塞时, 之后的内容将永远不会再同步
- 写入文件时, 会有延迟, 当monitor获取到的写请求频率过低时, 会每秒钟刷新一次文件到磁盘


## 目前默认同步的命令

- 只包含了 redis 官方文档(2023-03-01)中的 Generic, String, Hash, List, Set, Sorted Set 这六部分中的所有写操作
- 对于阻塞命令(如 BLPOP), 虽然目的端是多线程在写, 但还是建议尽量避免阻塞命令的出现
- 如果还需要同步其他命令, 请指定`额外命令`参数, 例如: `-additional-redis-commands="GRAPH.QUERY,JSON.SET"`

```
COPY
DEL
EXPIRE
EXPIREAT
MOVE
PERSIST
PEXPIRE
PEXPIREAT
RENAME
RENAMENX
RESTORE
SORT
TOUCH
UNLINK
APPEND
DECR
DECRBY
GETDEL
GETEX
GETSET
INCR
INCRBY
INCRBYFLOAT
MSET
MSETNX
PSETEX
SET
SETEX
SETNX
SETRANGE
HDEL
HINCRBY
HINCRBYFLOAT
HMSET
HSET
HSETNX
BLMOVE
BLMPOP
BLPOP
BRPOP
BRPOPLPUSH
LINSERT
LMOVE
LMPOP
LPOP
LPUSH
LPUSHX
LREM
LSET
LTRIM
RPOP
RPOPLPUSH
RPUSH
RPUSHX
SADD
SDIFFSTORE
SINTERSTORE
SMOVE
SPOP
SREM
SUNIONSTORE
BZMPOP
BZPOPMAX
BZPOPMIN
ZADD
ZDIFFSTORE
ZINCRBY
ZINTERSTORE
ZMPOP
ZPOPMAX
ZPOPMIN
ZRANGESTORE
ZREM
ZREMRANGEBYLEX
ZREMRANGEBYRANK
ZREMRANGEBYSCORE
ZUNIONSTORE
```
