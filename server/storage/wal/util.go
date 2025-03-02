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

package wal

import (
	"errors"
	"fmt"
	"strings"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"

	"go.uber.org/zap"
)

var errBadWALName = errors.New("bad wal name")

// Exist returns true if there are any files in a given directory.
/***
当前文件夹是否有wal文件
*/
func Exist(dir string) bool {
	names, err := fileutil.ReadDir(dir, fileutil.WithExt(".wal"))
	if err != nil {
		return false
	}
	return len(names) != 0
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
/*** 获取names中index小于等于输入index的序号
参数：names必须是升序的
*/
func searchIndex(lg *zap.Logger, names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		_, curIndex, err := parseWALName(name)
		if err != nil {
			lg.Panic("failed to parse WAL file name", zap.String("path", name), zap.Error(err))
		}
		if index >= curIndex {
			return i, true
		}
	}
	return -1, false
}

// names should have been sorted based on sequence number.
// isValidSeq checks whether seq increases continuously.
/***
校验names中Seq序号值是不是逐一递增的
*/
func isValidSeq(lg *zap.Logger, names []string) bool {
	var lastSeq uint64
	for _, name := range names {
		curSeq, _, err := parseWALName(name)
		if err != nil {
			lg.Panic("failed to parse WAL file name", zap.String("path", name), zap.Error(err))
		}
		if lastSeq != 0 && lastSeq != curSeq-1 {
			return false
		}
		lastSeq = curSeq
	}
	return true
}

/***
获取当前目录下的所有wal文件名称
*/
func readWALNames(lg *zap.Logger, dirpath string) ([]string, error) {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return nil, err
	}
	wnames := checkWalNames(lg, names)
	if len(wnames) == 0 {
		return nil, ErrFileNotFound
	}
	return wnames, nil
}

/***
获取输入names中有效的wal文件名称
*/
func checkWalNames(lg *zap.Logger, names []string) []string {
	wnames := make([]string, 0)
	for _, name := range names {
		if _, _, err := parseWALName(name); err != nil {
			// don't complain about left over tmp files
			if !strings.HasSuffix(name, ".tmp") {
				lg.Warn(
					"ignored file in WAL directory",
					zap.String("path", name),
				)
			}
			continue
		}
		wnames = append(wnames, name)
	}
	return wnames
}

/***
通过wal文件名提取seq和index的序号信息
*/
func parseWALName(str string) (seq, index uint64, err error) {
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, errBadWALName
	}
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}

/***
得到一个wal文件名称
*/
func walName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}
