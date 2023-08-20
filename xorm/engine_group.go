// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import "C"
import (
	//"github.com/yyythinking/xormplus/xorm/log"
	"github.com/yyythinking/xormplus/xorm/names"

	//"os"
	//"reflect"
	//"runtime"
	//"sync"
	//"time"
	"github.com/yyythinking/xormplus/core"
)

// EngineGroup defines an engine group
type EngineGroup struct {
	*Engine
	subordinates []*Engine
	policy       GroupPolicy
}

// NewEngineGroup creates a new engine group
func NewPostgresEngineGroup(args1 interface{}, policies ...GroupPolicy) (*EngineGroup, error) {
	var eg EngineGroup
	if len(policies) > 0 {
		eg.policy = policies[0]
	} else {
		eg.policy = RoundRobinPolicy()
	}

	conns, ok1 := args1.([]string)
	if ok1 {
		engines := make([]*Engine, len(conns))
		for i, conn := range conns {
			engine, err := NewPostgreSQL(conn)
			if err != nil {
				return nil, ErrParamsType
			}

			engine.engineGroup = &eg
			engines[i] = engine
		}

		eg.Engine = engines[0]
		eg.subordinates = engines[1:]
		return &eg, nil
	}
	return nil, ErrParamsType
}

func (engine *EngineGroup) SetSqlTemplateRootDir(sqlTemplateRootDir string) *EngineGroup {
	engine.sqlTemplate.SqlTemplateRootDir = sqlTemplateRootDir
	for i := 0; i < len(engine.subordinates); i++ {
		engine.subordinates[i].SetSqlTemplateRootDir(sqlTemplateRootDir)
	}
	return engine
}

func (engine *EngineGroup) InitSqlTemplate(options SqlTemplateOptions) error {
	err := engine.Engine.InitSqlTemplate(options)
	if err != nil {
		return err
	}
	for i := 0; i < len(engine.subordinates); i++ {
		err = engine.subordinates[i].InitSqlTemplate(options)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close the engine
func (eg *EngineGroup) Close() error {
	err := eg.Engine.Close()
	if err != nil {
		return err
	}

	for i := 0; i < len(eg.subordinates); i++ {
		err := eg.subordinates[i].Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Subordinates returns all the subordinates
func (eg *EngineGroup) Subordinates() []*Engine {
	return eg.subordinates
}

// NewSession returned a group session
func (eg *EngineGroup) NewSession() *Session {
	sess := eg.Engine.NewSession()
	//sess.sessionType = groupSession
	return sess
}

// Subordinate returns one of the physical databases which is a subordinate according the policy
func (eg *EngineGroup) Subordinate() *Engine {
	switch len(eg.subordinates) {
	case 0:
		return eg.Engine
	case 1:
		if err := eg.subordinates[0].Ping(); err == nil {
			return eg.subordinates[0]
		} else {
			return eg.Engine
		}
	}
	engine := eg.policy.Subordinate(eg)
	if err := engine.Ping(); err != nil {
		for i := 0; i < len(eg.subordinates); i++ {
			engine = eg.subordinates[i]
			if err := engine.Ping(); err == nil {
				break
			}
		}
	}
	return eg.Engine
}

// SetMaxIdleConns set the max idle connections on pool, default is 2
func (eg *EngineGroup) SetMaxIdleConns(conns int) {
	eg.Engine.DB().SetMaxIdleConns(conns)
	for i := 0; i < len(eg.subordinates); i++ {
		eg.subordinates[i].DB().SetMaxIdleConns(conns)
	}
}

// SetMaxOpenConns is only available for go 1.2+
func (eg *EngineGroup) SetMaxOpenConns(conns int) {
	eg.Engine.DB().SetMaxOpenConns(conns)
	for i := 0; i < len(eg.subordinates); i++ {
		eg.subordinates[i].DB().SetMaxOpenConns(conns)
	}
}

// SetTableMapper set the table name mapping rule
func (eg *EngineGroup) SetTableMapper(mapper names.Mapper) {
	eg.Engine.SetTableMapper(mapper)
	for i := 0; i < len(eg.subordinates); i++ {
		eg.subordinates[i].SetTableMapper(mapper)
	}
}

// ShowSQL show SQL statement or not on logger if log level is great than INFO
func (eg *EngineGroup) ShowSQL(show ...bool) {
	eg.Engine.ShowSQL(show...)
	for i := 0; i < len(eg.subordinates); i++ {
		eg.subordinates[i].ShowSQL(show...)
	}
}

// SetLogger set the new logger
func (eg *EngineGroup) SetLogger(logger core.ILogger) {
	eg.Engine.SetLogger(logger)
	for i := 0; i < len(eg.subordinates); i++ {
		eg.subordinates[i].SetLogger(logger)
	}
}

// SetLogLevel sets the logger level
func (eg *EngineGroup) SetLogLevel(level core.LogLevel) {
	eg.Engine.Logger().SetLevel(level)
	for i := 0; i < len(eg.subordinates); i++ {
		eg.subordinates[i].Logger().SetLevel(level)
	}
}
