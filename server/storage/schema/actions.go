// Copyright 2021 The etcd Authors
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

package schema

import (
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.uber.org/zap"
)

type action interface {
	// unsafeDo executes the action and returns revert action, when executed
	// should restore the state from before.
	/*** 执行action，返回一个回退action，如果返回结果被执行，将恢复到这个函数没有执行前的状态的
	 */
	unsafeDo(tx backend.BatchTx) (revert action, err error)
}

/***
给value设置一个值，包含了如果key之前不存在，则是添加，否则就是更改
*/
type setKeyAction struct {
	Bucket     backend.Bucket
	FieldName  []byte
	FieldValue []byte
}

/*** 执行SetKeyAction
- 得到一个revert的Action
- 执行put操作
*/
func (a setKeyAction) unsafeDo(tx backend.BatchTx) (action, error) {
	revert := restoreFieldValueAction(tx, a.Bucket, a.FieldName)
	tx.UnsafePut(a.Bucket, a.FieldName, a.FieldValue)
	return revert, nil
}

type deleteKeyAction struct {
	Bucket    backend.Bucket
	FieldName []byte
}

/*** 执行delete操作
- 获取revert Action
- 执行delete
*/
func (a deleteKeyAction) unsafeDo(tx backend.BatchTx) (action, error) {
	revert := restoreFieldValueAction(tx, a.Bucket, a.FieldName)
	tx.UnsafeDelete(a.Bucket, a.FieldName)
	return revert, nil
}

/***
- 如果在执行前可以查询到值，说明之前存在值，所以revert的Action就是将key对应的value更新到之前的值
- 否则的话，就是添加key，那么复原的时候对应的就是delete操作
*/
func restoreFieldValueAction(tx backend.BatchTx, bucket backend.Bucket, fieldName []byte) action {
	_, vs := tx.UnsafeRange(bucket, fieldName, nil, 1)
	if len(vs) == 1 {
		return &setKeyAction{
			Bucket:     bucket,
			FieldName:  fieldName,
			FieldValue: vs[0],
		}
	}
	return &deleteKeyAction{
		Bucket:    bucket,
		FieldName: fieldName,
	}
}

type ActionList []action

// unsafeExecute executes actions one by one. If one of actions returns error,
// it will revert them.
/*** 执行Action列表，如果其中一个失败，将会回退他们
 */
func (as ActionList) unsafeExecute(lg *zap.Logger, tx backend.BatchTx) error {
	var revertActions = make(ActionList, 0, len(as))
	for _, a := range as {
		revert, err := a.unsafeDo(tx)

		if err != nil {
			revertActions.unsafeExecuteInReversedOrder(lg, tx)
			return err
		}
		revertActions = append(revertActions, revert)
	}
	return nil
}

// unsafeExecuteInReversedOrder executes actions in revered order. Will panic on
// action error. Should be used when reverting.
func (as ActionList) unsafeExecuteInReversedOrder(lg *zap.Logger, tx backend.BatchTx) {
	for j := len(as) - 1; j >= 0; j-- {
		_, err := as[j].unsafeDo(tx)
		if err != nil {
			lg.Panic("Cannot recover from revert error", zap.Error(err))
		}
	}
}
