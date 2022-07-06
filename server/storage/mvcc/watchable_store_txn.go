// Copyright 2017 The etcd Authors
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
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
)

/***
根据写事务的变更，生成event列表
调用s.notify(rev, evs)方法，将event发送给watcher
*/
func (tw *watchableStoreTxnWrite) End() {
	changes := tw.Changes()
	if len(changes) == 0 {
		tw.TxnWrite.End()
		return
	}

	// tw.Rev() 返回的是开始事务前的revision
	// +1 表示当前写完成后的revision
	rev := tw.Rev() + 1
	evs := make([]mvccpb.Event, len(changes))
	for i, change := range changes {
		evs[i].Kv = &changes[i]
		if change.CreateRevision == 0 {
			evs[i].Type = mvccpb.DELETE
			evs[i].Kv.ModRevision = rev
		} else {
			evs[i].Type = mvccpb.PUT
		}
	}

	// end write txn under watchable store lock so the updates are visible
	// when asynchronous event posting checks the current store revision
	tw.s.mu.Lock()
	tw.s.notify(rev, evs)
	tw.TxnWrite.End()
	tw.s.mu.Unlock()
}

type watchableStoreTxnWrite struct {
	TxnWrite
	s *watchableStore
}

/***
创建一个带有watchable功能的TxnWrite，主要功能是在写txn完成后会主动通知store中的watcher
*/
func (s *watchableStore) Write(trace *traceutil.Trace) TxnWrite {
	return &watchableStoreTxnWrite{s.store.Write(trace), s}
}
