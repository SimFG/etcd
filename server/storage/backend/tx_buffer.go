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

package backend

import (
	"bytes"
	"sort"
)

const bucketBufferInitialSize = 512

// txBuffer handles functionality shared between txWriteBuffer and txReadBuffer.
type txBuffer struct {
	buckets map[BucketID]*bucketBuffer
}

/*** 清除bucket
1. 循环bucket的map，如果buffer还没有开始使用，则直接移出bucket，然后将used置为0
Tips: 每次往buffer里面添加元素，都是使用used作为下标进行添加，所以used为0表示在buffer里面没有有效缓存
*/
func (txb *txBuffer) reset() {
	for k, v := range txb.buckets {
		if v.used == 0 {
			// demote
			delete(txb.buckets, k)
		}
		v.used = 0
	}
}

// txWriteBuffer buffers writes of pending updates that have not yet committed.
/***
TODO simfg bucket2seq什么作用

bucket2seq 标识put进来的元素是否有序？
*/
type txWriteBuffer struct {
	txBuffer
	// Map from bucket ID into information whether this bucket is edited
	// sequentially (i.e. keys are growing monotonically).
	bucket2seq map[BucketID]bool
}

func (txw *txWriteBuffer) put(bucket Bucket, k, v []byte) {
	txw.bucket2seq[bucket.ID()] = false
	txw.putInternal(bucket, k, v)
}

func (txw *txWriteBuffer) putSeq(bucket Bucket, k, v []byte) {
	// TODO: Add (in tests?) verification whether k>b[len(b)]
	txw.putInternal(bucket, k, v)
}

func (txw *txWriteBuffer) putInternal(bucket Bucket, k, v []byte) {
	b, ok := txw.buckets[bucket.ID()]
	if !ok {
		b = newBucketBuffer()
		txw.buckets[bucket.ID()] = b
	}
	b.add(k, v)
}

/***
1. 将bucket内的buffer清除
2. 遍历bucket2seq，如果元素不存在与bucket内，则删除该元素；如果获取的buf的used为0，则将对应位置的元素置为true
*/
func (txw *txWriteBuffer) reset() {
	txw.txBuffer.reset()
	for k := range txw.bucket2seq {
		_, ok := txw.buckets[k]
		if !ok { // TODO simfg 什么情况下会存在这种情况，bucket2seq有，但是buckets没有
			delete(txw.bucket2seq, k)
			//} else if v.used == 0 { // TODO simfg confuse 因为txw.txBuffer.reset()已经将所有的used置为0，这部分是不是不用再一次做判断
			//	txw.bucket2seq[k] = true
			//}

		} else {
			txw.bucket2seq[k] = true
		}
	}
}

/*** 将当前buf的数据写入到readBuf中
1. 遍历bucket数组
1.1	查看输入txr里面是否有遍历元素，如果没有，删除当前bucket，并将对应的元素赋值给输入
1.2 查看bucket2seq对应的元素，如果为false且bucket里的buffer存在元素，则进行排序
1.3 将当前buf元素合并到输入的buf中
2. 重置当前buf

TODO simfg confuse 这里的sort，因为buf中的数据是通过used进行标识，但是排序的时候直接使用的序号，这里会不会导致将reset的元素被重新使用呢
*/
func (txw *txWriteBuffer) writeback(txr *txReadBuffer) {
	for k, wb := range txw.buckets {
		rb, ok := txr.buckets[k]
		if !ok {
			delete(txw.buckets, k)
			txr.buckets[k] = wb
			continue
		}
		if seq, ok := txw.bucket2seq[k]; ok && !seq && wb.used > 1 {
			// assume no duplicate keys
			sort.Sort(wb)
		}
		rb.merge(wb)
	}
	txw.reset()
	// increase the buffer version
	txr.bufVersion++
}

/***
txBuffer 一个map，key为bucketId，value为bucketBuffer，bucketBuffer里面是一个数组
*/
// txReadBuffer accesses buffered updates.
/***
bufVersion TODO simfg 有什么作用
*/
type txReadBuffer struct {
	txBuffer
	// bufVersion is used to check if the buffer is modified recently
	bufVersion uint64
}

/***
根据输入bucket获取到对应的buffer，然后找出key/end范围内的值
*/
func (txr *txReadBuffer) Range(bucket Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if b := txr.buckets[bucket.ID()]; b != nil {
		return b.Range(key, endKey, limit)
	}
	return nil, nil
}

/***
根据输入bucket获取到对应的buffer，然后对buffer内的值都调用visitor函数
*/
func (txr *txReadBuffer) ForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	if b := txr.buckets[bucket.ID()]; b != nil {
		return b.ForEach(visitor)
	}
	return nil
}

// unsafeCopy returns a copy of txReadBuffer, caller should acquire backend.readTx.RLock()
func (txr *txReadBuffer) unsafeCopy() txReadBuffer {
	txrCopy := txReadBuffer{
		txBuffer: txBuffer{
			buckets: make(map[BucketID]*bucketBuffer, len(txr.txBuffer.buckets)),
		},
		bufVersion: 0,
	}
	for bucketName, bucket := range txr.txBuffer.buckets {
		txrCopy.txBuffer.buckets[bucketName] = bucket.Copy()
	}
	return txrCopy
}

type kv struct {
	key []byte
	val []byte
}

// bucketBuffer buffers key-value pairs that are pending commit.
type bucketBuffer struct {
	buf []kv
	// used tracks number of elements in use so buf can be reused without reallocation.
	used int
}

/***
创建一个bucketBuffer，其内部的buf数组直接被指定大小，后续赋值使用buf[index]进行赋值
*/
func newBucketBuffer() *bucketBuffer {
	return &bucketBuffer{buf: make([]kv, bucketBufferInitialSize), used: 0}
}

/***
获取key/endKey之间的值
1. 在buf中获取key的索引
2. 如果endKey不存在，则比较索引对应的key与输入key
3. 获取buf中在key, endKey之间的值
*/
func (bb *bucketBuffer) Range(key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	f := func(i int) bool { return bytes.Compare(bb.buf[i].key, key) >= 0 }
	/***
	二分法搜索，找到buf数组中最靠左的大于等于key的值
	*/
	idx := sort.Search(bb.used, f)
	if idx < 0 || idx >= bb.used {
		return nil, nil
	}
	if len(endKey) == 0 {
		if bytes.Equal(key, bb.buf[idx].key) {
			keys = append(keys, bb.buf[idx].key)
			vals = append(vals, bb.buf[idx].val)
		}
		return keys, vals
	}
	if bytes.Compare(endKey, bb.buf[idx].key) <= 0 {
		return nil, nil
	}
	for i := idx; i < bb.used && int64(len(keys)) < limit; i++ {
		if bytes.Compare(endKey, bb.buf[i].key) <= 0 {
			break
		}
		keys = append(keys, bb.buf[i].key)
		vals = append(vals, bb.buf[i].val)
	}
	return keys, vals
}

/***
使用输入的visitor函数访问所有的buf元素
*/
func (bb *bucketBuffer) ForEach(visitor func(k, v []byte) error) error {
	for i := 0; i < bb.used; i++ {
		if err := visitor(bb.buf[i].key, bb.buf[i].val); err != nil {
			return err
		}
	}
	return nil
}

/***添加缓存
如果添加缓存后，buf数组没有容量，则进行1.5倍扩容
*/
func (bb *bucketBuffer) add(k, v []byte) {
	bb.buf[bb.used].key, bb.buf[bb.used].val = k, v
	bb.used++
	if bb.used == len(bb.buf) {
		buf := make([]kv, (3*len(bb.buf))/2)
		copy(buf, bb.buf)
		bb.buf = buf
	}
}

// merge merges data from bbsrc into bb.
/***
1. 将输入的buf所有内容添加到当前缓存中
2. 如果当前缓存长度为0，直接返回
3. 如果在未添加元素钱当前缓存的最后key值小于输入buf的第一个key，直接返回
4. 给元素进行排序
5. 去除重复元素
Tips: buf数组中的元素是有序的
*/
func (bb *bucketBuffer) merge(bbsrc *bucketBuffer) {
	for i := 0; i < bbsrc.used; i++ {
		bb.add(bbsrc.buf[i].key, bbsrc.buf[i].val)
	}
	if bb.used == bbsrc.used {
		return
	}
	if bytes.Compare(bb.buf[(bb.used-bbsrc.used)-1].key, bbsrc.buf[0].key) < 0 {
		return
	}

	sort.Stable(bb)

	// remove duplicates, using only newest update
	widx := 0
	for ridx := 1; ridx < bb.used; ridx++ {
		if !bytes.Equal(bb.buf[ridx].key, bb.buf[widx].key) {
			widx++
		}
		bb.buf[widx] = bb.buf[ridx]
	}
	bb.used = widx + 1
}

func (bb *bucketBuffer) Len() int { return bb.used }
func (bb *bucketBuffer) Less(i, j int) bool {
	return bytes.Compare(bb.buf[i].key, bb.buf[j].key) < 0
}
func (bb *bucketBuffer) Swap(i, j int) { bb.buf[i], bb.buf[j] = bb.buf[j], bb.buf[i] }

func (bb *bucketBuffer) Copy() *bucketBuffer {
	bbCopy := bucketBuffer{
		buf:  make([]kv, len(bb.buf)),
		used: bb.used,
	}
	copy(bbCopy.buf, bb.buf)
	return &bbCopy
}
