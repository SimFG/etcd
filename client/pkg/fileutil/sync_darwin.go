// Copyright 2016 The etcd Authors
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

//go:build darwin
// +build darwin

package fileutil

import (
	"os"

	"golang.org/x/sys/unix"
)

// Fsync on HFS/OSX flushes the data on to the physical drive but the drive
// may not write it to the persistent media for quite sometime and it may be
// written in out-of-order sequence. Using F_FULLFSYNC ensures that the
// physical drive's buffer will also get flushed to the media.
// HFS/OSX 上的 Fsync 将数据刷新到物理驱动器，但驱动器可能在相当长的一段时间内不会将其写入持久媒体，并且可能会以乱序顺序写入。 使用 F_FULLFSYNC 可确保物理驱动器的缓冲区也将刷新到媒体。
func Fsync(f *os.File) error {
	_, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0)
	return err
}

// Fdatasync on darwin platform invokes fcntl(F_FULLFSYNC) for actual persistence
// on physical drive media.
func Fdatasync(f *os.File) error {
	return Fsync(f)
}
