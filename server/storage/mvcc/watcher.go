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
	"bytes"
	"errors"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// AutoWatchID is the watcher ID passed in WatchStream.Watch when no
// user-provided ID is available. If pass, an ID will automatically be assigned.
const AutoWatchID WatchID = 0

var (
	ErrWatcherNotExist    = errors.New("mvcc: watcher does not exist")
	ErrEmptyWatcherRange  = errors.New("mvcc: watcher range is empty")
	ErrWatcherDuplicateID = errors.New("mvcc: duplicate watch ID provided on the WatchStream")
)

type WatchID int64

// FilterFunc returns true if the given event should be filtered out.
type FilterFunc func(e mvccpb.Event) bool

type WatchStream interface {
	// Watch creates a watcher. The watcher watches the events happening or
	// happened on the given key or range [key, end) from the given startRev.
	//
	// The whole event history can be watched unless compacted.
	// If "startRev" <=0, watch observes events after currentRev.
	//
	// The returned "id" is the ID of this watcher. It appears as WatchID
	// in events that are sent to the created watcher through stream channel.
	// The watch ID is used when it's not equal to AutoWatchID. Otherwise,
	// an auto-generated watch ID is returned.
	Watch(id WatchID, key, end []byte, startRev int64, fcs ...FilterFunc) (WatchID, error)

	// Chan returns a chan. All watch response will be sent to the returned chan.
	Chan() <-chan WatchResponse

	// RequestProgress requests the progress of the watcher with given ID. The response
	// will only be sent if the watcher is currently synced.
	// The responses will be sent through the WatchRespone Chan attached
	// with this stream to ensure correct ordering.
	// The responses contains no events. The revision in the response is the progress
	// of the watchers since the watcher is currently synced.
	RequestProgress(id WatchID)

	// Cancel cancels a watcher by giving its ID. If watcher does not exist, an error will be
	// returned.
	Cancel(id WatchID) error

	// Close closes Chan and release all related resources.
	Close()

	// Rev returns the current revision of the KV the stream watches on.
	Rev() int64
}

type WatchResponse struct {
	// WatchID is the WatchID of the watcher this response sent to.
	WatchID WatchID

	// Events contains all the events that needs to send.
	Events []mvccpb.Event

	// Revision is the revision of the KV when the watchResponse is created.
	// For a normal response, the revision should be the same as the last
	// modified revision inside Events. For a delayed response to a unsynced
	// watcher, the revision is greater than the last modified revision
	// inside Events.
	Revision int64

	// CompactRevision is set when the watcher is cancelled due to compaction.
	CompactRevision int64
}

// watchStream contains a collection of watchers that share
// one streaming chan to send out watched events and other control events.
// 服务端stream，连接serverWatchStream和WatcherKV
type watchStream struct {
	watchable watchable
	ch        chan WatchResponse

	mu sync.Mutex // guards fields below it
	// nextID is the ID pre-allocated for next new watcher in this stream
	nextID   WatchID
	closed   bool
	cancels  map[WatchID]cancelFunc
	watchers map[WatchID]*watcher
}

// Watch creates a new watcher in the stream and returns its WatchID.
/*** 创建一个watcher
- 判断key end是否是一个合法的字符串范围
- stream 是否关闭
- 生成watcherId
	- 如果是自生成watcherId，则根据nextID自增找到一个位置
	- 如果是指定的，则判断该位置是否存在watcher
- 调用watchable生成watcher，并保存watcher和其对应取消的函数
*/
func (ws *watchStream) Watch(id WatchID, key, end []byte, startRev int64, fcs ...FilterFunc) (WatchID, error) {
	// prevent wrong range where key >= end lexicographically
	// watch request with 'WithFromKey' has empty-byte range end
	if len(end) != 0 && bytes.Compare(key, end) != -1 {
		return -1, ErrEmptyWatcherRange
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.closed {
		return -1, ErrEmptyWatcherRange
	}

	if id == AutoWatchID {
		for ws.watchers[ws.nextID] != nil {
			ws.nextID++
		}
		id = ws.nextID
		// TODO simfg pr
	} else if _, ok := ws.watchers[id]; ok {
		return -1, ErrWatcherDuplicateID
	}

	w, c := ws.watchable.watch(key, end, startRev, id, ws.ch, fcs...)

	ws.cancels[id] = c
	ws.watchers[id] = w
	return id, nil
}

func (ws *watchStream) Chan() <-chan WatchResponse {
	return ws.ch
}

/*** 取消某个watcher
- 根据id获取watcher
- 判断watcher是否存在
- 调用取消
- 然后删除watchers和cancels列表
*/
func (ws *watchStream) Cancel(id WatchID) error {
	ws.mu.Lock()
	cancel, ok := ws.cancels[id]
	w := ws.watchers[id]
	ok = ok && !ws.closed
	ws.mu.Unlock()

	if !ok {
		return ErrWatcherNotExist
	}
	cancel()

	ws.mu.Lock()
	// The watch isn't removed until cancel so that if Close() is called,
	// it will wait for the cancel. Otherwise, Close() could close the
	// watch channel while the store is still posting events.
	if ww := ws.watchers[id]; ww == w {
		delete(ws.cancels, id)
		delete(ws.watchers, id)
	}
	ws.mu.Unlock()

	return nil
}

/***
关闭当前stream
- 根据cancels列表中，调用cancel方法
- 更新closed值
*/
func (ws *watchStream) Close() {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	for _, cancel := range ws.cancels {
		cancel()
	}
	ws.closed = true
	close(ws.ch)
	watchStreamGauge.Dec()
}

func (ws *watchStream) Rev() int64 {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.watchable.rev()
}

/***
查看watcher的请求进度
返回当前watcher处于物理时钟的哪一个revision
*/
func (ws *watchStream) RequestProgress(id WatchID) {
	ws.mu.Lock()
	w, ok := ws.watchers[id]
	ws.mu.Unlock()
	if !ok {
		return
	}
	ws.watchable.progress(w)
}
