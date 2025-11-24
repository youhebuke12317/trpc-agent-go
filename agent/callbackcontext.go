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
	"errors"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// CallbackContext 提供一个类型化的包装器，用于上下文与 agent 特定的操作。
// 与 ADK Python 的 callback_context 类似，它提供了会话范围的运行时操作，例如管理 artifact。
type CallbackContext struct {
	context.Context
	invocation *Invocation
	// State 是当前会话的增量感知状态。
	State session.StateMap
}

// NewCallbackContext 从标准上下文中创建一个 CallbackContext。
// 如果上下文中没有找到调用，则返回错误。
func NewCallbackContext(ctx context.Context) (*CallbackContext, error) {
	invocation, ok := InvocationFromContext(ctx)
	if !ok || invocation == nil {
		return nil, errors.New("invocation not found in context")
	}
	var state = make(session.StateMap)
	if invocation.Session != nil && invocation.Session.State != nil {
		state = invocation.Session.State
	}
	return &CallbackContext{
		Context:    ctx,
		invocation: invocation,
		State:      state,
	}, nil
}

// SaveArtifact 保存一个 artifact 并记录当前会话
//
// Args:
//   - filename: artifact 的文件名
//   - artifact: 要保存的 artifact
//
// Returns:
//   - artifact 的版本
func (cc *CallbackContext) SaveArtifact(filename string, artifact *artifact.Artifact) (int, error) {
	service, sessionInfo, err := cc.getArtifactServiceAndSessionInfo()
	if err != nil {
		return 0, err
	}
	return service.SaveArtifact(cc.Context, sessionInfo, filename, artifact)
}

// LoadArtifact 加载当前会话附加的 artifact。
//
// Args:
//   - filename: artifact 的文件名
//   - version: artifact 的版本。如果为 nil，则返回最新版本。
//
// Returns:
//   - artifact，如果未找到则返回 nil
func (cc *CallbackContext) LoadArtifact(filename string, version *int) (*artifact.Artifact, error) {
	service, sessionInfo, err := cc.getArtifactServiceAndSessionInfo()
	if err != nil {
		return nil, err
	}
	return service.LoadArtifact(cc.Context, sessionInfo, filename, version)
}

// ListArtifacts 列出当前会话附加的 artifact 的文件名。
//
// Returns:
//   - artifact 的文件名列表
func (cc *CallbackContext) ListArtifacts() ([]string, error) {
	service, sessionInfo, err := cc.getArtifactServiceAndSessionInfo()
	if err != nil {
		return nil, err
	}
	return service.ListArtifactKeys(cc.Context, sessionInfo)
}

// DeleteArtifact 从当前会话中删除一个 artifact。
// Args:
//   - filename: 要删除的 artifact 的文件名
func (cc *CallbackContext) DeleteArtifact(filename string) error {
	service, sessionInfo, err := cc.getArtifactServiceAndSessionInfo()
	if err != nil {
		return err
	}
	return service.DeleteArtifact(cc.Context, sessionInfo, filename)
}

// ListArtifactVersions 列出 artifact 的所有版本。
//
// Args:
//   - filename: artifact 的文件名
//
// Returns:
//   - artifact 的所有版本列表
func (cc *CallbackContext) ListArtifactVersions(filename string) ([]int, error) {
	service, sessionInfo, err := cc.getArtifactServiceAndSessionInfo()
	if err != nil {
		return nil, err
	}
	return service.ListVersions(cc.Context, sessionInfo, filename)
}

// getArtifactServiceAndSessionInfo 获取 artifact service 和 session 的通用逻辑
func (cc *CallbackContext) getArtifactServiceAndSessionInfo() (s artifact.Service, sessionInfo artifact.SessionInfo, err error) {
	service := cc.invocation.ArtifactService
	if service == nil {
		return nil, artifact.SessionInfo{}, errors.New("artifact service is nil in invocation")
	}

	appName, userID, sessionID, err := cc.appUserSession()
	if err != nil {
		return nil, artifact.SessionInfo{}, err
	}

	sessionInfo = artifact.SessionInfo{
		AppName:   appName,
		UserID:    userID,
		SessionID: sessionID,
	}

	return service, sessionInfo, nil
}

// appUserSession 从 Invocation 中提取 app 名称、用户 ID 和会话 ID
func (cc *CallbackContext) appUserSession() (appName, userID, sessionID string, err error) {
	// 尝试从 session 中获取
	if cc.invocation.Session == nil {
		return "", "", "", errors.New("invocation exists but no session available")
	}

	// session 中有 AppName 和 UserID 字段
	if cc.invocation.Session.AppName != "" && cc.invocation.Session.UserID != "" && cc.invocation.Session.ID != "" {
		return cc.invocation.Session.AppName, cc.invocation.Session.UserID, cc.invocation.Session.ID, nil
	}

	// 如果 session 存在但缺少所需的字段，则返回错误
	return "", "", "", fmt.Errorf("session exists but missing appName or userID or sessionID: appName=%s, userID=%s, sessionID=%s",
		cc.invocation.Session.AppName, cc.invocation.Session.UserID, cc.invocation.Session.ID)
}
