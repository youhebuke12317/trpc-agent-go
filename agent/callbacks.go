//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//
//

// Package agent 提供了核心 agent 功能
package agent

import (
	"context"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// ErrorTypeAgentCallbackError 是用于 agent 回调（before/after）中的错误类型。
const ErrorTypeAgentCallbackError = "agent_callback_error"

// BeforeAgentCallback 是在 agent 运行之前调用的回调函数。
// 返回 (customResponse, error)。
// - customResponse：如果不为空，则此响应将被返回给用户，并且将跳过 agent 执行。
// - error：如果不为空，则 agent 将停止运行并返回此错误。
type BeforeAgentCallback func(ctx context.Context, invocation *Invocation) (*model.Response, error)

// AfterAgentCallback 是在 agent 运行之后调用的回调函数。
// 返回 (customResponse, error)。
// - customResponse：如果不为空，则此响应将被用于实际的 agent 响应。
// - error：如果不为空，则将返回此错误。
type AfterAgentCallback func(ctx context.Context, invocation *Invocation, runErr error) (*model.Response, error)

// Callbacks 用于 agent 操作的回调。
type Callbacks struct {
	// BeforeAgent is a list of callbacks that are called before the agent runs.
	// BeforeAgent 是在 agent 运行之前调用的回调函数列表。
	BeforeAgent []BeforeAgentCallback
	// AfterAgent is a list of callbacks that are called after the agent runs.
	// AfterAgent 是在 agent 运行之后调用的回调函数列表。
	AfterAgent []AfterAgentCallback
}

// NewCallbacks 创建一个用于 agent 的 Callbacks 实例。
func NewCallbacks() *Callbacks {
	return &Callbacks{}
}

// RegisterBeforeAgent 注册一个 agent 运行前的回调函数。
func (c *Callbacks) RegisterBeforeAgent(cb BeforeAgentCallback) *Callbacks {
	c.BeforeAgent = append(c.BeforeAgent, cb)
	return c
}

// RegisterAfterAgent 注册一个 agent 运行后的回调函数。
func (c *Callbacks) RegisterAfterAgent(cb AfterAgentCallback) *Callbacks {
	c.AfterAgent = append(c.AfterAgent, cb)
	return c
}

// RunBeforeAgent 按顺序运行所有 agent 运行前的回调函数。
// 返回 (customResponse, error)。
// 如果任何回调返回自定义响应，则停止并返回。
func (c *Callbacks) RunBeforeAgent(
	ctx context.Context,
	invocation *Invocation,
) (*model.Response, error) {
	for _, cb := range c.BeforeAgent {
		customResponse, err := cb(ctx, invocation)
		if err != nil {
			return nil, err
		}
		if customResponse != nil {
			return customResponse, nil
		}
	}
	return nil, nil
}

// RunAfterAgent 按顺序运行所有 agent 运行后的回调函数。
// 返回 (customResponse, error)。
// 如果任何回调返回自定义响应，则停止并返回。
func (c *Callbacks) RunAfterAgent(
	ctx context.Context,
	invocation *Invocation,
	runErr error,
) (*model.Response, error) {
	for _, cb := range c.AfterAgent {
		customResponse, err := cb(ctx, invocation, runErr)
		if err != nil {
			return nil, err
		}
		if customResponse != nil {
			return customResponse, nil
		}
	}
	return nil, nil
}
