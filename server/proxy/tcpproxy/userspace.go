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

package tcpproxy

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// SRV 服务定位记录，是域名系统中用于指定服务器提供服务的位置（如主机名和端口）数据
// https://zh.m.wikipedia.org/zh-hans/SRV%E8%AE%B0%E5%BD%95
type remote struct {
	mu       sync.Mutex
	srv      *net.SRV
	addr     string
	inactive bool
}

func (r *remote) inactivate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inactive = true
}

func (r *remote) tryReactivate() error {
	conn, err := net.Dial("tcp", r.addr)
	if err != nil {
		return err
	}
	conn.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inactive = false
	return nil
}

func (r *remote) isActive() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.inactive
}

type TCPProxy struct {
	Logger          *zap.Logger
	Listener        net.Listener
	Endpoints       []*net.SRV
	MonitorInterval time.Duration

	donec chan struct{}

	mu        sync.Mutex // guards the following fields
	remotes   []*remote
	pickCount int // for round robin
}

// The parameter host is returned by net.SplitHostPort previously,
// so it must be a valid host. This function is only to check whether
// it's an IPv6 IP address.
func isIPv6(host string) bool {
	return strings.IndexRune(host, ':') != -1
}

// A literal IPv6 address in hostport must be enclosed in square
// brackets, as in "[::1]:80", "[::1%lo0]:80".
func formatAddr(host string, port uint16) string {
	if isIPv6(host) {
		return fmt.Sprintf("[%s]:%d", host, port)
	}
	return fmt.Sprintf("%s:%d", host, port)
}

func (tp *TCPProxy) Run() error {
	tp.donec = make(chan struct{})
	if tp.MonitorInterval == 0 {
		tp.MonitorInterval = 5 * time.Minute
	}
	for _, srv := range tp.Endpoints {
		addr := formatAddr(srv.Target, srv.Port)
		tp.remotes = append(tp.remotes, &remote{srv: srv, addr: addr})
	}

	eps := []string{}
	for _, ep := range tp.Endpoints {
		eps = append(eps, fmt.Sprintf("%s:%d", ep.Target, ep.Port))
	}
	if tp.Logger != nil {
		tp.Logger.Info("ready to proxy client requests", zap.Strings("endpoints", eps))
	}

	go tp.runMonitor()
	for {
		in, err := tp.Listener.Accept()
		if err != nil {
			return err
		}

		go tp.serve(in)
	}
}

/***
选择一个服务节点，会参考Priority和Weight
*/
func (tp *TCPProxy) pick() *remote {
	var weighted []*remote
	var unweighted []*remote

	bestPr := uint16(65535)
	w := 0
	// find best priority class
	for _, r := range tp.remotes {
		switch {
		case !r.isActive():
		case r.srv.Priority < bestPr:
			bestPr = r.srv.Priority
			w = 0
			weighted = nil
			unweighted = nil
			fallthrough
		case r.srv.Priority == bestPr:
			if r.srv.Weight > 0 {
				weighted = append(weighted, r)
				w += int(r.srv.Weight)
			} else {
				unweighted = append(unweighted, r)
			}
		}
	}
	if weighted != nil {
		if len(unweighted) > 0 && rand.Intn(100) == 1 {
			// In the presence of records containing weights greater
			// than 0, records with weight 0 should have a very small
			// chance of being selected.
			r := unweighted[tp.pickCount%len(unweighted)]
			tp.pickCount++
			return r
		}
		// choose a uniform random number between 0 and the sum computed
		// (inclusive), and select the RR whose running sum value is the
		// first in the selected order
		choose := rand.Intn(w)
		for i := 0; i < len(weighted); i++ {
			choose -= int(weighted[i].srv.Weight)
			if choose <= 0 {
				return weighted[i]
			}
		}
	}
	if unweighted != nil {
		for i := 0; i < len(tp.remotes); i++ {
			picked := tp.remotes[tp.pickCount%len(tp.remotes)]
			tp.pickCount++
			if picked.isActive() {
				return picked
			}
		}
	}
	return nil
}

func (tp *TCPProxy) serve(in net.Conn) {
	var (
		err error
		out net.Conn
	)

	for {
		tp.mu.Lock()
		remote := tp.pick()
		tp.mu.Unlock()
		if remote == nil {
			break
		}
		// TODO: add timeout
		out, err = net.Dial("tcp", remote.addr)
		if err == nil {
			break
		}
		remote.inactivate()
		if tp.Logger != nil {
			tp.Logger.Warn("deactivated endpoint", zap.String("address", remote.addr), zap.Duration("interval", tp.MonitorInterval), zap.Error(err))
		}
	}

	if out == nil {
		in.Close()
		return
	}

	go func() {
		io.Copy(in, out)
		in.Close()
		out.Close()
	}()

	io.Copy(out, in)
	out.Close()
	in.Close()
}

/***
保活，固定间隔检测服务列表的状态，如果服务不可用了，尝试重连
*/
func (tp *TCPProxy) runMonitor() {
	for {
		select {
		case <-time.After(tp.MonitorInterval):
			tp.mu.Lock()
			for _, rem := range tp.remotes {
				if rem.isActive() {
					continue
				}
				go func(r *remote) {
					if err := r.tryReactivate(); err != nil {
						if tp.Logger != nil {
							tp.Logger.Warn("failed to activate endpoint (stay inactive for another interval)", zap.String("address", r.addr), zap.Duration("interval", tp.MonitorInterval), zap.Error(err))
						}
					} else {
						if tp.Logger != nil {
							tp.Logger.Info("activated", zap.String("address", r.addr))
						}
					}
				}(rem)
			}
			tp.mu.Unlock()
		case <-tp.donec:
			return
		}
	}
}

func (tp *TCPProxy) Stop() {
	// graceful shutdown?
	// shutdown current connections?
	tp.Listener.Close()
	close(tp.donec)
}
