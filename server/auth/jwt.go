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

package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/rsa"
	"errors"
	"time"

	jwt "github.com/golang-jwt/jwt"
	"go.uber.org/zap"
)

/***
verifyOnly 含义：是否为公钥，仅用于验证
*/
type tokenJWT struct {
	lg         *zap.Logger
	signMethod jwt.SigningMethod
	key        interface{}
	ttl        time.Duration
	verifyOnly bool
}

func (t *tokenJWT) enable()                         {}
func (t *tokenJWT) disable()                        {}
func (t *tokenJWT) invalidateUser(string)           {}
func (t *tokenJWT) genTokenPrefix() (string, error) { return "", nil }

/*** 提取token中的信息
- 格式化token，然后验证
	- 对比签名方法是否一致
	- 获取key
- 从token中提取用户名称和版本信息
*/
func (t *tokenJWT) info(ctx context.Context, token string, rev uint64) (*AuthInfo, bool) {
	// rev isn't used in JWT, it is only used in simple token
	var (
		username string
		revision uint64
	)

	parsed, err := jwt.Parse(token, func(token *jwt.Token) (interface{}, error) {
		if token.Method.Alg() != t.signMethod.Alg() {
			return nil, errors.New("invalid signing method")
		}
		switch k := t.key.(type) {
		case *rsa.PrivateKey:
			return &k.PublicKey, nil
		case *ecdsa.PrivateKey:
			return &k.PublicKey, nil
		default:
			return t.key, nil
		}
	})

	if err != nil {
		t.lg.Warn(
			"failed to parse a JWT token",
			zap.Error(err),
		)
		return nil, false
	}

	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !parsed.Valid || !ok {
		t.lg.Warn("failed to obtain claims from a JWT token")
		return nil, false
	}

	username = claims["username"].(string)
	revision = uint64(claims["revision"].(float64))

	return &AuthInfo{Username: username, Revision: revision}, true
}

/***
使用tokenJWT加密信息，并返回相应的token
*/
func (t *tokenJWT) assign(ctx context.Context, username string, revision uint64) (string, error) {
	if t.verifyOnly {
		return "", ErrVerifyOnly
	}

	// Future work: let a jwt token include permission information would be useful for
	// permission checking in proxy side.
	tk := jwt.NewWithClaims(t.signMethod,
		jwt.MapClaims{
			"username": username,
			"revision": revision,
			"exp":      time.Now().Add(t.ttl).Unix(),
		})

	token, err := tk.SignedString(t.key)
	if err != nil {
		t.lg.Debug(
			"failed to sign a JWT token",
			zap.String("user-name", username),
			zap.Uint64("revision", revision),
			zap.Error(err),
		)
		return "", err
	}

	t.lg.Debug(
		"created/assigned a new JWT token",
		zap.String("user-name", username),
		zap.Uint64("revision", revision),
		zap.String("token", token),
	)
	return token, err
}

/***
- 提取map中的信息，赋值到jwtOption中，包括了过期时间，公钥，私钥，签名方法
- 收集map中多余的key
- 提取签名的key
- 创建tokenJWT，然后根据情况赋值verifyOnly
*/
func newTokenProviderJWT(lg *zap.Logger, optMap map[string]string) (*tokenJWT, error) {
	if lg == nil {
		lg = zap.NewNop()
	}
	var err error
	var opts jwtOptions
	err = opts.ParseWithDefaults(optMap)
	if err != nil {
		lg.Error("problem loading JWT options", zap.Error(err))
		return nil, ErrInvalidAuthOpts
	}

	var keys = make([]string, 0, len(optMap))
	for k := range optMap {
		if !knownOptions[k] {
			keys = append(keys, k)
		}
	}
	if len(keys) > 0 {
		lg.Warn("unknown JWT options", zap.Strings("keys", keys))
	}

	key, err := opts.Key()
	if err != nil {
		return nil, err
	}

	t := &tokenJWT{
		lg:         lg,
		ttl:        opts.TTL,
		signMethod: opts.SignMethod,
		key:        key,
	}

	switch t.signMethod.(type) {
	case *jwt.SigningMethodECDSA:
		if _, ok := t.key.(*ecdsa.PublicKey); ok {
			t.verifyOnly = true
		}
	case *jwt.SigningMethodRSA, *jwt.SigningMethodRSAPSS:
		if _, ok := t.key.(*rsa.PublicKey); ok {
			t.verifyOnly = true
		}
	}

	return t, nil
}
