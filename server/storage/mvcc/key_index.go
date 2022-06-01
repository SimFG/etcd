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
	"fmt"

	"github.com/google/btree"
	"go.uber.org/zap"
)

var (
	ErrRevisionNotFound = errors.New("mvcc: revision not found")
)

// keyIndex stores the revisions of a key in the backend.
// Each keyIndex has at least one key generation.
// Each generation might have several key versions.
// Tombstone on a key appends an tombstone version at the end
// of the current generation and creates a new empty generation.
// Each version of a key has an index pointing to the backend.
//
// For example: put(1.0);put(2.0);tombstone(3.0);put(4.0);tombstone(5.0) on key "foo"
// generate a keyIndex:
// key:     "foo"
// rev: 5
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {1.0, 2.0, 3.0(t)}
//
// Compact a keyIndex removes the versions with smaller or equal to
// rev except the largest one. If the generation becomes empty
// during compaction, it will be removed. if all the generations get
// removed, the keyIndex should be removed.
//
// For example:
// compact(2) on the previous example
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {2.0, 3.0(t)}
//
// compact(4)
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//
// compact(5):
// generations:
//    {empty} -> key SHOULD be removed.
//
// compact(6):
// generations:
//    {empty} -> key SHOULD be removed.
/***
key. 也就是put的时候的key字符串对于的数组
modified. 最近修改的revision,
	revision. 表示版本
		main. ectd的逻辑时钟
		sub. 一个事务中的执行序号
generations. key对应的generation数组
	generation. 一个key从创建到删除记录
		ver. 修改次数
		created. 当前generation创建的版本号
		revisions. 每一次修改的revision记录
*/
type keyIndex struct {
	key         []byte
	modified    revision // the main rev of the last modification
	generations []generation
}

// put puts a revision to the keyIndex.
/*** 给keyIndex添加一个revision记录
1. 这里没有将key传进来是因为在创建keyIndex的时候，会将key进行初始化
2. 校验当前reversion是否比上次修改的revision大
3. 如果generations为空，添加一个generation元素
4. 获取generations最新的一个元素，如果这个generation没有reversion，初始化created属性的reversion
5. 将当前reversion添加到generation的reversions数组，然后自增修改次数ver
6. 更新keyIndex的最近修改revision属性modified
*/
func (ki *keyIndex) put(lg *zap.Logger, main int64, sub int64) {
	rev := revision{main: main, sub: sub}

	if !rev.GreaterThan(ki.modified) {
		lg.Panic(
			"'put' with an unexpected smaller revision",
			zap.Int64("given-revision-main", rev.main),
			zap.Int64("given-revision-sub", rev.sub),
			zap.Int64("modified-revision-main", ki.modified.main),
			zap.Int64("modified-revision-sub", ki.modified.sub),
		)
	}
	if len(ki.generations) == 0 {
		ki.generations = append(ki.generations, generation{})
	}
	g := &ki.generations[len(ki.generations)-1]
	if len(g.revs) == 0 { // create a new key
		keysGauge.Inc()
		g.created = rev
	}
	g.revs = append(g.revs, rev)
	g.ver++
	ki.modified = rev
}

/*** 给keyIndex的generations数组添加一个generation
created参数 -> generation.created
modified参数 -> generation.revs = []revision{modified}
ver参数 -> generation.ver
*/
func (ki *keyIndex) restore(lg *zap.Logger, created, modified revision, ver int64) {
	if len(ki.generations) != 0 {
		lg.Panic(
			"'restore' got an unexpected non-empty generations",
			zap.Int("generations-size", len(ki.generations)),
		)
	}

	ki.modified = modified
	g := generation{created: created, ver: ver, revs: []revision{modified}}
	ki.generations = append(ki.generations, g)
	keysGauge.Inc()
}

// tombstone puts a revision, pointing to a tombstone, to the keyIndex.
// It also creates a new empty generation in the keyIndex.
// It returns ErrRevisionNotFound when tombstone on an empty generation.
/*** 结束当前KeyIndex的generations，表示当前key已经被删除
1. 校验keyIndex是否为空，即keyIndex的generations数组长度为1且数组中的generation是否为空（revs的长度为0）
2. generations数组中最新的generation是否为空（revs的长度为0）
3. 给keyIndex添加一个revision
4. 向keyIndex的generations添加空generation元素
*/
func (ki *keyIndex) tombstone(lg *zap.Logger, main int64, sub int64) error {
	if ki.isEmpty() {
		lg.Panic(
			"'tombstone' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}
	if ki.generations[len(ki.generations)-1].isEmpty() {
		return ErrRevisionNotFound
	}
	ki.put(lg, main, sub)
	ki.generations = append(ki.generations, generation{})
	keysGauge.Dec()
	return nil
}

// get gets the modified, created revision and version of the key that satisfies the given atRev.
// Rev must be smaller than or equal to the given atRev.
/*** 获取atRev最近的修改版本信息，如果atRev是过期generation的最后一个值或者是generation之间的值都返回为空
1. 检验当前keyIndex是否为空，空表示之前没有添加过revision记录
2. 找到当前atRev所在的generation
3. 如果找到的generation为空，返回错误
4. 遍历generation中的revision数组，找到一个小于等于atRev的最大revision，
	找到了返回这个revision，创建当前generation的revision，和这个key是这个generation创建以来的第几个版本
TODO simfg 为啥不直接用n+1呢？
举个例子：
当前generation [[1,3,5],[10,12,15],[20,25]]
输入atRev:13
返回: modified是12对应的revision，created是10对应的revision，而ver是2，表示从创建以来的第二个版本
*/
func (ki *keyIndex) get(lg *zap.Logger, atRev int64) (modified, created revision, ver int64, err error) {
	if ki.isEmpty() {
		lg.Panic(
			"'get' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}
	g := ki.findGeneration(atRev)
	if g.isEmpty() {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}

	n := g.walk(func(rev revision) bool { return rev.main > atRev })
	if n != -1 {
		return g.revs[n], g.created, g.ver - int64(len(g.revs)-n-1), nil
	}

	return revision{}, revision{}, 0, ErrRevisionNotFound
}

// since returns revisions since the given rev. Only the revision with the
// largest sub revision will be returned if multiple revisions have the same
// main revision.
/***
1. 判断当前keyIndex是否为空，也就是是否有revision数据
2. 根据输入rev构建revision
3. 从最后一个generation开始遍历，找到第一个参数revision大于generation的创建revision，记录当前generation的序号
4. 从找到的generation开始遍历，将revision大于等于rev地都收集起来，最后进行返回
TODO simfg 为什么在最后一个收集的过程中会出现相邻的两个revision相等，也就是if r.main == last {}这个语句
*/
func (ki *keyIndex) since(lg *zap.Logger, rev int64) []revision {
	if ki.isEmpty() {
		lg.Panic(
			"'since' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}
	since := revision{rev, 0}
	var gi int
	// find the generations to start checking
	for gi = len(ki.generations) - 1; gi > 0; gi-- {
		g := ki.generations[gi]
		if g.isEmpty() {
			continue
		}
		if since.GreaterThan(g.created) {
			break
		}
	}

	var revs []revision
	var last int64
	for ; gi < len(ki.generations); gi++ {
		for _, r := range ki.generations[gi].revs {
			if since.GreaterThan(r) {
				continue
			}
			if r.main == last {
				// replace the revision with a new one that has higher sub value,
				// because the original one should not be seen by external
				revs[len(revs)-1] = r
				continue
			}
			revs = append(revs, r)
			last = r.main
		}
	}
	return revs
}

// compact compacts a keyIndex by removing the versions with smaller or equal
// revision than the given atRev except the largest one (If the largest one is
// a tombstone, it will not be kept).
// If a generation becomes empty during compaction, it will be removed.
/***
1. 判断keyIndex是否为空
2. 找出距离atRev最近的过期generation和revision的索引，如果keyIndex的generations数组中只有一个元素，那这个就不是过期的
3. 获取到generation，根据获取到revision索引，将其之前的版本全部删除
4. 如果删除后，这个generation只剩下一个revision元素，且这个generation是过期的，那么将这个generation也删除，然后将之前互获取的genIdx自增一下
5. 最后在删除generations数组中的元素
Tips: 这里的available会记录删除后最新的一个revision，如果刚好过期的generation只有一个revision，那么这个就不会存在available变量中
*/
func (ki *keyIndex) compact(lg *zap.Logger, atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		lg.Panic(
			"'compact' got an unexpected empty keyIndex",
			zap.String("key", string(ki.key)),
		)
	}

	genIdx, revIndex := ki.doCompact(atRev, available)

	g := &ki.generations[genIdx]
	if !g.isEmpty() {
		// remove the previous contents.
		if revIndex != -1 {
			g.revs = g.revs[revIndex:]
		}
		// remove any tombstone
		if len(g.revs) == 1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[0])
			genIdx++
		}
	}

	// remove the previous generations.
	ki.generations = ki.generations[genIdx:]
}

/***
1. 判断keyIndex是否为空
2. 找出距离atRev最近的过期generation和revision的索引
3. 如果获得revision是过期generation的最后一个，那么将revision的值从available中移除
*/
// keep finds the revision to be kept if compact is called at given atRev.
func (ki *keyIndex) keep(atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		return
	}

	genIdx, revIndex := ki.doCompact(atRev, available)
	g := &ki.generations[genIdx]
	if !g.isEmpty() {
		// remove any tombstone
		if revIndex == len(g.revs)-1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[revIndex])
		}
	}
}

/*** 根据atRev，在过期的generation中找出距离atRev最近的generation的序号及其在这个generation中小于等于atRev的最大值
1. 创建函数f，找到第一个小于等于atRev的revision，并将这个revision放入available
2. 获取keyIndex中第一个generation，并声明generation的序号
3. 开始遍历，不包括keyIndex的最后一个generation，因为这个是正在使用
3.1 判断generation的最后一个revision是否大于atRev，如果是，跳出循环
3.2 如果不是，更新genIdx和g，进入下一次循环
4. 遍历g中的revision，找出小于等于atRev的最大revision，并返回revision的索引
*/
func (ki *keyIndex) doCompact(atRev int64, available map[revision]struct{}) (genIdx int, revIndex int) {
	// walk until reaching the first revision smaller or equal to "atRev",
	// and add the revision to the available map
	f := func(rev revision) bool {
		if rev.main <= atRev {
			available[rev] = struct{}{}
			return false
		}
		return true
	}

	genIdx, g := 0, &ki.generations[0]
	// find first generation includes atRev or created after atRev
	for genIdx < len(ki.generations)-1 {
		if tomb := g.revs[len(g.revs)-1].main; tomb > atRev {
			break
		}
		genIdx++
		g = &ki.generations[genIdx]
	}

	revIndex = g.walk(f)

	return genIdx, revIndex
}

func (ki *keyIndex) isEmpty() bool {
	return len(ki.generations) == 1 && ki.generations[0].isEmpty()
}

// findGeneration finds out the generation of the keyIndex that the
// given rev belongs to. If the given rev is at the gap of two generations,
// which means that the key does not exist at the given rev, it returns nil.
/*** 根据参数rev获取这个rev在哪一个generation中
将generation看做一个revision的集合，revision核心其实就是一个main版本号

1. 获取generations的长度，用于遍历，遍历是从数组最后一个元素开始的
2. 对于每一个generation，遍历过程是
2.1 判断当前generation中是否存在元素，也就是revs的长度判断
2.2 获取当前generation，如果generation不是最后一个，也就是说是之前删除过的generation，
	这里会判断输出参数rev是否是大于等于这个generation的最后一个元素，如果是那么就返回nil，
	原因是因为如果不是keyIndex中generations中的最后一个generation，那么这个generation最后的
	revision就是表示删除的revision，这里可以看看generation方法
2.3 判断这个generation的第一个元素是不是小于等于rev，如果是则返回这个generation
Tips: 为什么2.2这个，为啥大于等于呢？
举个例子：
- 假如，keyIndex存在的generations有三个，分别为[1,2,5],[10,12,15][20,25,30]
a. 输入rev为21，自然返回最后一个generation
b. 输入rev为18，这个时候返回的就是nil，这个时候就是2.2步骤里的大于起作用
c. 输入rev为15，这个时候返回的就是nil，为啥不是返回第二个generation，这个是因为这个generation是已经过期的，
	那么最后一个revision表示已删除的revision，也就是在这个revision的时候，这个key被删除了
*/
func (ki *keyIndex) findGeneration(rev int64) *generation {
	lastg := len(ki.generations) - 1
	cg := lastg

	for cg >= 0 {
		if len(ki.generations[cg].revs) == 0 {
			cg--
			continue
		}
		g := ki.generations[cg]
		if cg != lastg {
			if tomb := g.revs[len(g.revs)-1].main; tomb <= rev {
				return nil
			}
		}
		if g.revs[0].main <= rev {
			return &ki.generations[cg]
		}
		cg--
	}
	return nil
}

/***
字母排序
*/
func (ki *keyIndex) Less(b btree.Item) bool {
	return bytes.Compare(ki.key, b.(*keyIndex).key) == -1
}

func (ki *keyIndex) equal(b *keyIndex) bool {
	if !bytes.Equal(ki.key, b.key) {
		return false
	}
	if ki.modified != b.modified {
		return false
	}
	if len(ki.generations) != len(b.generations) {
		return false
	}
	for i := range ki.generations {
		ag, bg := ki.generations[i], b.generations[i]
		if !ag.equal(bg) {
			return false
		}
	}
	return true
}

func (ki *keyIndex) String() string {
	var s string
	for _, g := range ki.generations {
		s += g.String()
	}
	return s
}

// generation contains multiple revisions of a key.
type generation struct {
	ver     int64
	created revision // when the generation is created (put in first revision).
	revs    []revision
}

func (g *generation) isEmpty() bool { return g == nil || len(g.revs) == 0 }

// walk walks through the revisions in the generation in descending order.
// It passes the revision to the given function.
// walk returns until: 1. it finishes walking all pairs 2. the function returns false.
// walk returns the position at where it stopped. If it stopped after
// finishing walking, -1 will be returned.
/*** 遍历generation中的revision数组，从最后一个开始
如果f函数返回false，则返回当前generation中的revision的序号
*/
func (g *generation) walk(f func(rev revision) bool) int {
	l := len(g.revs)
	for i := range g.revs {
		ok := f(g.revs[l-i-1])
		if !ok {
			return l - i - 1
		}
	}
	return -1
}

func (g *generation) String() string {
	return fmt.Sprintf("g: created[%d] ver[%d], revs %#v\n", g.created, g.ver, g.revs)
}

func (g generation) equal(b generation) bool {
	if g.ver != b.ver {
		return false
	}
	if len(g.revs) != len(b.revs) {
		return false
	}

	for i := range g.revs {
		ar, br := g.revs[i], b.revs[i]
		if ar != br {
			return false
		}
	}
	return true
}
