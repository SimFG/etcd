// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mvcc

import (
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

/***
finishedCompact，这个表示已经完成compact的版本，在执行完一部分db的compact的操作后会更新值，见：文件kvstore_compaction.go store.scheduleCompaction L62
*/
func UnsafeReadFinishedCompact(tx backend.ReadTx) (finishedComact int64, found bool) {
	_, finishedCompactBytes := tx.UnsafeRange(schema.Meta, schema.FinishedCompactKeyName, nil, 0)
	if len(finishedCompactBytes) != 0 {
		return bytesToRev(finishedCompactBytes[0]).main, true
	}
	return 0, false
}

/***
scheduledComact，这个表示当前正在准备进行compact操作，在进行compact操作之前，会先设置这个值，见：kvstore.go store.updateCompactRev L255
*/
func UnsafeReadScheduledCompact(tx backend.ReadTx) (scheduledComact int64, found bool) {
	_, scheduledCompactBytes := tx.UnsafeRange(schema.Meta, schema.ScheduledCompactKeyName, nil, 0)
	if len(scheduledCompactBytes) != 0 {
		return bytesToRev(scheduledCompactBytes[0]).main, true
	}
	return 0, false
}

func SetScheduledCompact(tx backend.BatchTx, value int64) {
	tx.LockInsideApply()
	defer tx.Unlock()
	UnsafeSetScheduledCompact(tx, value)
}

func UnsafeSetScheduledCompact(tx backend.BatchTx, value int64) {
	rbytes := newRevBytes()
	revToBytes(revision{main: value}, rbytes)
	tx.UnsafePut(schema.Meta, schema.ScheduledCompactKeyName, rbytes)
}

func SetFinishedCompact(tx backend.BatchTx, value int64) {
	tx.LockInsideApply()
	defer tx.Unlock()
	UnsafeSetFinishedCompact(tx, value)
}

func UnsafeSetFinishedCompact(tx backend.BatchTx, value int64) {
	rbytes := newRevBytes()
	revToBytes(revision{main: value}, rbytes)
	tx.UnsafePut(schema.Meta, schema.FinishedCompactKeyName, rbytes)
}
