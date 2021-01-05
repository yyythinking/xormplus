// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"math/rand"
	"sync"
	"time"
)

// GroupPolicy is be used by chosing the current subordinate from subordinates
type GroupPolicy interface {
	Subordinate(*EngineGroup) *Engine
}

// GroupPolicyHandler should be used when a function is a GroupPolicy
type GroupPolicyHandler func(*EngineGroup) *Engine

// Subordinate implements the chosen of subordinates
func (h GroupPolicyHandler) Subordinate(eg *EngineGroup) *Engine {
	return h(eg)
}

// RoundRobinPolicy returns a group policy handler
func RoundRobinPolicy() GroupPolicyHandler {
	var pos = -1
	var lock sync.Mutex
	return func(g *EngineGroup) *Engine {
		var subordinates = g.Subordinates()

		lock.Lock()
		defer lock.Unlock()
		pos++
		if pos >= len(subordinates) {
			pos = 0
		}

		return subordinates[pos]
	}
}

// RandomPolicy implmentes randomly chose the subordinate of subordinates
func RandomPolicy() GroupPolicyHandler {
	var r = rand.New(rand.NewSource(time.Now().UnixNano()))
	return func(g *EngineGroup) *Engine {
		return g.Subordinates()[r.Intn(len(g.Subordinates()))]
	}
}