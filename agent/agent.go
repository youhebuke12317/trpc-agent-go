//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//
//

// Package agent 提供了核心代理功能
package agent

import (
	"context"
	"errors"

	"trpc.group/trpc-go/trpc-agent-go/codeexecutor"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// Info 包含的 agent 基本信息
type Info struct {
	// Name 是 agent 的名称
	Name string
	// Description 是 agent 的描述
	Description string
	// InputSchema 是 agent 的输入协议
	InputSchema map[string]any
	// OutputSchema 是 agent 的输出协议
	OutputSchema map[string]any
}

// ErrorTypeStopAgentError 用于表示应该停止执行
const ErrorTypeStopAgentError = "stop_agent_error"

// StopError 表示一个错误，提示 agent 执行应停止. 当此错误类型返回时，表示 agent 应停止处理。
type StopError struct {
	// Message 包含停止原因
	Message string
}

// Error 实现了 error 接口
func (e *StopError) Error() string {
	return e.Message
}

// AsStopError  通过 errors.As 检查错误是否为 StopError.
func AsStopError(err error) (*StopError, bool) {
	var stopErr *StopError
	ok := errors.As(err, &stopErr)
	return stopErr, ok
}

// NewStopError 创建一个 StopError 实例，包含指定的消息
func NewStopError(message string) *StopError {
	return &StopError{Message: message}
}

// Agent 是所有 agent 都必须实现的接口. 定义了 agent 的基本接口，包括执行和工具方法
type Agent interface {
	// Run 在给定上下文中执行所提供的调用，并返回一个代表执行进度和结果的事件通道.
	Run(ctx context.Context, invocation *Invocation) (<-chan *event.Event, error)

	// Tools 返回该 agent 可访问并可执行的工具列表。这些工具代表 agent 在调用时可用的能力
	Tools() []tool.Tool

	// Info 返回关于此 agent 的基本信息
	Info() Info

	// SubAgents 返回此 agent 可用的 sub-agent 列表。如果没有可用的 sub-agent，则返回空切片。
	SubAgents() []Agent

	// FindSubAgent 通过名称查找 sub-agent。如果找不到具有给定名称的 sub-agent，则返回 nil。
	FindSubAgent(name string) Agent
}

// CodeExecutor 用于执行代码块. 可能会迁移到 Agent 接口，
// 这将引发大规模变化，稍后再考虑。或迁移到 codeexecutor 包
type CodeExecutor interface {
	// CodeExecutor 返回此 agent 使用的代码执行器。这允许 agent 在不同的环境中执行代码块。
	CodeExecutor() codeexecutor.CodeExecutor
}
