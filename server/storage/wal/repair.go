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
	"io"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.uber.org/zap"
)

// Repair tries to repair ErrUnexpectedEOF in the
// last wal file by truncating.
/*** 修复 因为截断导致ErrUnexpectedEOF错误的 最后一个wal文件，返回true表示修复成功，false表示失败
- 获取最后一个wal文件
- 获取decoder
- 不断的读取wal文件内容至Record对象中
- 如果成功内容到Record对象中，通过cyc校验数据
- 如果是EOF，表示已经读取到文件末尾，直接返回true
- 如果获得异常，则
	- 创建一个新文件
	- 复制当前文件到新文件中
	- 对新文件进行截断（Truncate），其中文件有效位置则是lastOffset
	- 刷新文件
*/
func Repair(lg *zap.Logger, dirpath string) bool {
	if lg == nil {
		lg = zap.NewNop()
	}
	f, err := openLast(lg, dirpath)
	if err != nil {
		return false
	}
	defer f.Close()

	lg.Info("repairing", zap.String("path", f.Name()))

	rec := &walpb.Record{}
	decoder := newDecoder(fileutil.NewFileReader(f.File))
	for {
		lastOffset := decoder.lastOffset()
		err := decoder.decode(rec)
		switch err {
		case nil:
			// update crc of the decoder when necessary
			switch rec.Type {
			case crcType:
				crc := decoder.crc.Sum32()
				// current crc of decoder must match the crc of the record.
				// do no need to match 0 crc, since the decoder is a new one at this case.
				if crc != 0 && rec.Validate(crc) != nil {
					return false
				}
				decoder.updateCRC(rec.Crc)
			}
			continue

		case io.EOF:
			lg.Info("repaired", zap.String("path", f.Name()), zap.Error(io.EOF))
			return true

		case io.ErrUnexpectedEOF:
			brokenName := f.Name() + ".broken"
			bf, bferr := os.Create(brokenName)
			if bferr != nil {
				lg.Warn("failed to create backup file", zap.String("path", brokenName), zap.Error(bferr))
				return false
			}
			defer bf.Close()

			if _, err = f.Seek(0, io.SeekStart); err != nil {
				lg.Warn("failed to read file", zap.String("path", f.Name()), zap.Error(err))
				return false
			}

			if _, err = io.Copy(bf, f); err != nil {
				lg.Warn("failed to copy", zap.String("from", f.Name()), zap.String("to", brokenName), zap.Error(err))
				return false
			}

			if err = f.Truncate(lastOffset); err != nil {
				lg.Warn("failed to truncate", zap.String("path", f.Name()), zap.Error(err))
				return false
			}

			start := time.Now()
			if err = fileutil.Fsync(f.File); err != nil {
				lg.Warn("failed to fsync", zap.String("path", f.Name()), zap.Error(err))
				return false
			}
			walFsyncSec.Observe(time.Since(start).Seconds())

			lg.Info("repaired", zap.String("path", f.Name()), zap.Error(io.ErrUnexpectedEOF))
			return true

		default:
			lg.Warn("failed to repair", zap.String("path", f.Name()), zap.Error(err))
			return false
		}
	}
}

// openLast opens the last wal file for read and write.
/***
读取最后一个wal的文件，用于读和写
*/
func openLast(lg *zap.Logger, dirpath string) (*fileutil.LockedFile, error) {
	names, err := readWALNames(lg, dirpath)
	if err != nil {
		return nil, err
	}
	last := filepath.Join(dirpath, names[len(names)-1])
	return fileutil.LockFile(last, os.O_RDWR, fileutil.PrivateFileMode)
}
