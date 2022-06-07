# etcd 阅读

![etcd Logo](logos/etcd-horizontal-color.svg)

## 推荐阅读

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