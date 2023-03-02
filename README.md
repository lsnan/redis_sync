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
  示例 : 

    将本机 redis 6379 实例的 monitor 输出到 redis.log 文件:
	  redis_sync -outfile=redis.log

    将远程 redis 6380 实例的 monitor 输出到 redis.log 文件:
	  redis_sync --source-host='10.10.10.10' -source-port=6380 -source-password='xxxxxxxxxx' --output='file' -outfile=redis.log

    将远程 redis 6380 实例的 monitor 同步到远程 redis 6381:
	  redis_sync --source-host='10.10.10.10' -source-port=6380 -source-password='xxxxxxxxxx' --output='redis' --dest-host='11.11.11.11' --dest-port=6381 --dest-password='xxxxxxxx'

    将远程 redis 6380 实例的 monitor 同步到远程 redis 6381, 并且写入本地文件:
	  redis_sync --source-host='10.10.10.10' -source-port=6380 -source-password='xxxxxxxxxx' --output='both' --dest-host='11.11.11.11' --dest-port=6381 --dest-password='xxxxxxxx' -outfile=redis.log
```

## Features

- 将 redis monitor 获取到的相关命令同步在目标 redis 库执行
- 将 redis monitor 获取到的相关命令输出到指定文件 

## 注意事项

- 不同步原始的基础数据, 从运行 redis_sync 命令开始同步 monitor 看到的命令(即只有增量)。
- 源端 redis 连接异常, 程序会直接退出
- 目的端 redis 连接异常或写入异常, 不退出程序
- 目的端 redis 连接异常或写入异常, 丢弃当前条目, 继续下一条记录的写入
- `CTRL + C` 或源端异常后, 是立即退出程序, 没有等待处理已经从源端读取到本地缓存的数据  

## TODO

- [ ] 目的端 redis 连接异常或写入异常时, 是否需要添加直接退出程序的逻辑


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