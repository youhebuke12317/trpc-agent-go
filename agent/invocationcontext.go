//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//
//

package agent

import (
	"context"
)

// InvocationContext 携带调用信息
type InvocationContext struct {
	context.Context
}
type invocationKey struct{}

// NewInvocationContext 创建一个新的 InvocationContext
func NewInvocationContext(ctx context.Context, invocation *Invocation) *InvocationContext {
	return &InvocationContext{
		Context: context.WithValue(ctx, invocationKey{}, invocation),
	}
}

// InvocationFromContext 从上下文中返回调用信息
func InvocationFromContext(ctx context.Context) (*Invocation, bool) {
	invocation, ok := ctx.Value(invocationKey{}).(*Invocation)
	return invocation, ok
}

// CheckContextCancelled 检查上下文是否被取消
func CheckContextCancelled(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
