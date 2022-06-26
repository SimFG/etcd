# etcd 阅读
根据文件顺序阅读相关功能，在阅读过程中，如果遇到什么疑惑可以在discussions栏进行讨论。

![etcd Logo](logos/etcd-horizontal-color.svg)

## 推荐阅读

- [理解LSM Tree](https://mp.weixin.qq.com/s/7kdg7VQMxa4TsYqPfF8Yug)
- [LSM Tree 实现一个KV数据库](https://www.cnblogs.com/whuanle/p/16297025.html)
- [etcd 官方doc](https://etcd.io/docs/v3.5/)
- [模块介绍](https://etcd.io/docs/v3.5/dev-internal/modules/)
- [api介绍](https://www.lixueduan.com/post/etcd/03-v3-analyze/)

## 存储-storage

阅读
- [mvcc源码分析](https://www.lixueduan.com/post/etcd/12-mvcc-analyze/)
- [backend实现原理](https://blog.csdn.net/u010853261/article/details/109630223)

源码 
### index
维护键值对中key与revision的数据关系，内部使用了btree
- [key_index](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/mvcc/key_index.go)
- [tree_index](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/mvcc/index.go)
### backend
数据库存储，key为revision，value为键值对
- [tx_buffer](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/backend/tx_buffer.go)
- [read_tx](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/backend/read_tx.go)
- [batch_tx](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/backend/batch_tx.go)
- [metrics](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/backend/metrics.go)
- [backend](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/backend/backend.go)
### mvcc kv
- [pkg/schedule-fifo](https://github.com/SimFG/etcd-doc/blob/simfg-doc/pkg/schedule/schedule.go)
- [db_compact](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/mvcc/kvstore_compaction.go)
- [finishedCompact-scheduledCompact](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/mvcc/store.go)
- [kvstore](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/mvcc/kvstore.go)
### mvcc watcher
watcher功能的实现
- [pkg/adt-interval_tree](https://github.com/SimFG/etcd-doc/blob/simfg-doc/pkg/adt/interbal_tree.go)
- [watcher_group](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/mvcc/watcher_group.go)
- [watcher](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/mvcc/watcher.go)
- [watcher_store](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/mvcc/watchable_store.go)
- [watcher_store_txn](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/mvcc/watchable_store_txn.go)
### WAL
推荐文章

- [wal介绍](https://www.codedump.info/post/20210628-etcd-wal/)
- [wal关键流程分析](https://zhuanlan.zhihu.com/p/380378857)

实现一致性的重要手段之一
- [doc](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/wal/doc.go)
- [decoder-写数据](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/wal/decoder.go)
- [encoder-读数据](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/wal/encoder.go)
- [file-pipeline](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/wal/file_pipeline.go)
- [repair](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/wal/repair.go)
- [util](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/wal/util.go)
- [wal](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/wal/wal.go)
- [version](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/wal/version.go)
### Schema
主要是提供了一些操作Bucket的封装类
- [action](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/actions.go)
- [alarm](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/alarm.go)
- [auth](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/auth.go)
- [auth_roles](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/auth_roles.go)
- [auth-users](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/auth_users.go)
- [changes](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/changes.go)
- [cindex](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/cindex.go)
- [confstate](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/confstate.go)
- [lease](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/lease.go)
- [membership](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/membership.go)
- [migration](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/migration.go)
- [schema](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/schema.go)
- [version](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/storage/schema/version.go)
### auth
权限校验相关
- [jwt](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/auth/jwt.go)
- [simple_token](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/auth/simple_token.go)
- [nop](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/auth/nop.go)
- [range_perm_cache](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/auth/range_perm_cache.go)
- [store](https://github.com/SimFG/etcd-doc/blob/simfg-doc/server/auth/store.go)
### config log
配置zap log
- [config_logger](server/embed/config_logging.go)
- [config](server/embed/config.go)
- [config_trace](server/embed/config_tracing.go)
### proxy
主要是grpc的一些配置，下面主要涉及一些工具方法
- [wait](pkg/wait/wait.go)
- [wait_time](pkg/wait/wait_time.go)
- [userspace](server/proxy/tcpproxy/userspace.go)
- [store](server/proxy/grpcproxy/cache/store.go)
- [chan_stream](server/proxy/grpcproxy/adapter/chan_stream.go)
### lease
文章：[lease源码分析](https://www.modb.pro/db/79463)
- [lease](server/lease/lease.go)
- [lease_queue](server/lease/lease_queue.go)
- [http](server/lease/leasehttp/http.go)
- [lessor](server/lease/lessor.go)

## MR列表
阅读过程中，如果发现问题，可以etcd仓库提mr合入
- [proxy: Put the pb object into the struct](https://github.com/etcd-io/etcd/pull/14157)
- [verify: Get backend using simple api](https://github.com/etcd-io/etcd/pull/14153)
- [config: Add the default case when failing to parse the log rotate config json](https://github.com/etcd-io/etcd/pull/14146)
- [schedule: support to recover from job panic for the fifo](https://github.com/etcd-io/etcd/pull/14109)
- [wal: remove the repeated test case](https://github.com/etcd-io/etcd/pull/14106)
- [mvcc: improve the use of locks in index.go](https://github.com/etcd-io/etcd/pull/14084)

## 函数列表
- [net.SplitHostPort](server/embed/config.go) 获取url字符串的host port信息
- [os.Stat](client/pkg/fileutil/fileutil.go) 获取文件状态，可以用于校验文件权限
- [SelfCert](client/pkg/transport/listener.go#188) 用代码进行tls自签名，这样可以自动开启tls认证
- [DialJournal](client/pkg/systemd/journal.go) 与系统服务通信，可用于校验某个系统服务是否可用