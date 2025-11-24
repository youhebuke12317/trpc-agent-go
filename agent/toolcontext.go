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

// ToolContext 是用于工具调用的上下文
type ToolContext struct {
	// CallbackContext 回调上下文是回调的上下文。
	*CallbackContext
}

// NewToolContext 从给定的上下文中创建一个新的 ToolContext。
func NewToolContext(ctx context.Context) (*ToolContext, error) {
	cbCtx, err := NewCallbackContext(ctx)
	if err != nil {
		return nil, err
	}
	return &ToolContext{CallbackContext: cbCtx}, nil
}
