// Copyright 2018 The etcd Authors
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

//go:build !windows
// +build !windows

package embed

import (
	"fmt"
	"os"

	"go.etcd.io/etcd/client/pkg/v3/logutil"

	"go.uber.org/zap/zapcore"
)

// use stderr as fallback
// 使用系统服务来管理日志 https://blog.51cto.com/zaishu/5137218
func getJournalWriteSyncer() (zapcore.WriteSyncer, error) {
	jw, err := logutil.NewJournalWriter(os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("can't find journal (%v)", err)
	}
	return zapcore.AddSync(jw), nil
}
