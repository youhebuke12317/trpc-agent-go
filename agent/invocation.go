// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
package agent

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/searchfilter"
	"trpc.group/trpc-go/trpc-agent-go/log"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

const (
	// WaitNoticeWithoutTimeout 是无超时等待时的超时时长
	WaitNoticeWithoutTimeout = 0 * time.Second

	// AppendEventNoticeKeyPrefix 是 Append Event Notice 键的前缀
	AppendEventNoticeKeyPrefix = "append_event:"

	// BranchDelimiter is the delimiter for branch
	// BranchDelimiter 是分支的分隔符
	BranchDelimiter = "/"

	// EventFilterKeyDelimiter is the delimiter for event filter key
	// EventFilterKeyDelimiter 是事件过滤器键的分隔符
	EventFilterKeyDelimiter = "/"
)

// TransferInfo contains information about a pending agent transfer.
// TransferInfo 包含关于待处理 agent 转移的信息。
type TransferInfo struct {
	// TargetAgentName is the name of the agent to transfer control to.
	// TargetAgentName 是要转移控制权的 agent 名称。
	TargetAgentName string
	// Message is the message to send to the target agent.
	// Message 是要发送给目标 agent 的消息。
	Message string
}

// Invocation represents the context for a flow execution.
// Invocation 表示执行流的上下文。
type Invocation struct {
	// Agent is the agent that is being invoked.
	// Agent 是正在调用的 agent。
	Agent Agent
	// AgentName is the name of the agent that is being invoked.
	// AgentName 是正在调用的 agent 名称。
	AgentName string
	// InvocationID is the ID of the invocation.
	// InvocationID 是调用的 ID。
	InvocationID string
	// Branch records agent execution chain information.
	// In multi-agent mode, this is useful for tracing agent execution trajectories.
	// Branch 记录 agent 执行链信息。
	// 在 multi-agent 模式下，这对于追踪 agent 执行轨迹非常有用。
	Branch string
	// EndInvocation is a flag that indicates if the invocation is complete.
	// EndInvocation 是一个标志，表示调用是否完成。
	EndInvocation bool
	// Session is the session that is being used for the invocation.
	// Session 是正在用于调用的会话。
	Session *session.Session
	// Model is the model that is being used for the invocation.
	// Model 是正在用于调用的模型。
	Model model.Model
	// Message is the message that is being sent to the agent.
	// Message 是要发送给 agent 的消息。
	Message model.Message
	// RunOptions is the options for the Run method.
	// RunOptions 是 Run 方法的选项。
	RunOptions RunOptions
	// TransferInfo contains information about a pending agent transfer.
	// TransferInfo 包含有关待处理 agent 转移的信息。
	TransferInfo *TransferInfo

	// StructuredOutput defines how the model should produce structured output for this invocation.
	// StructuredOutput 定义模型如何为该调用生成结构化输出。
	StructuredOutput *model.StructuredOutput
	// StructuredOutputType is the Go type to unmarshal the final JSON into.
	// StructuredOutputType 是将最终 JSON 解析为的 Go 类型。
	StructuredOutputType reflect.Type

	// MemoryService is the service for managing memory.
	// MemoryService 是用于管理内存的服务。
	MemoryService memory.Service
	// ArtifactService is the service for managing artifacts.
	// ArtifactService 是用于管理 artifact 的服务。
	ArtifactService artifact.Service

	// noticeChanMap is used to signal when events are written to the session.
	// noticeChanMap 用于在将事件写入会话时发出信号。
	noticeChanMap map[string]chan any
	noticeMu      *sync.Mutex

	// eventFilterKey is used to filter events for flow or agent
	// eventFilterKey 用于过滤流或 agent 的事件。
	eventFilterKey string

	// parent is the parent invocation, if any
	// parent 是父调用，如果有
	parent *Invocation

	// state stores invocation-scoped state data (lazy initialized).
	// Can be used by callbacks, middleware, or any invocation-scoped logic.
	// state 存储调用范围的状态数据（惰性初始化）。可以由回调、中间件或任何调用范围逻辑使用。
	state   map[string]any
	stateMu sync.RWMutex
}

// DefaultWaitNoticeTimeoutErr is the default error returned when a wait notice times out.
// DefaultWaitNoticeTimeoutErr 是等待通知超时时返回的默认错误。
var DefaultWaitNoticeTimeoutErr = NewWaitNoticeTimeoutError("wait notice timeout.")

// WaitNoticeTimeoutError represents an error that signals the wait notice timeout.
// WaitNoticeTimeoutError 表示一个信号等待通知超时的错误。
type WaitNoticeTimeoutError struct {
	// Message contains the stop reason
	Message string
}

// Error implements the error interface.
// Error 实现了 error 接口。
func (e *WaitNoticeTimeoutError) Error() string {
	return e.Message
}

// AsWaitNoticeTimeoutError checks if an error is a AsWaitNoticeTimeoutError using errors.As.
// AsWaitNoticeTimeoutError 使用 errors.As 检查是否为 AsWaitNoticeTimeoutError。
func AsWaitNoticeTimeoutError(err error) (*WaitNoticeTimeoutError, bool) {
	var waitNoticeTimeoutErr *WaitNoticeTimeoutError
	ok := errors.As(err, &waitNoticeTimeoutErr)
	return waitNoticeTimeoutErr, ok
}

// NewWaitNoticeTimeoutError creates a new AsWaitNoticeTimeoutError with the given message.
// NewWaitNoticeTimeoutError 创建一个带有给定消息的 AsWaitNoticeTimeoutError。
func NewWaitNoticeTimeoutError(message string) *WaitNoticeTimeoutError {
	return &WaitNoticeTimeoutError{Message: message}
}

// RunOption is a function that configures a RunOptions.
// RunOption 是用于配置 RunOptions 的函数。
type RunOption func(*RunOptions)

// WithRuntimeState sets the runtime state for the RunOptions.
// WithRuntimeState 设置 RunOptions 的运行时状态。
func WithRuntimeState(state map[string]any) RunOption {
	return func(opts *RunOptions) {
		opts.RuntimeState = state
	}
}

// WithKnowledgeFilter sets the metadata filter for the RunOptions.
// WithKnowledgeFilter 设置 RunOptions 的元数据过滤器。
func WithKnowledgeFilter(filter map[string]any) RunOption {
	return func(opts *RunOptions) {
		opts.KnowledgeFilter = filter
	}
}

// WithKnowledgeConditionedFilter sets the complex condition filter for the RunOptions.
// WithKnowledgeConditionedFilter 设置复杂条件过滤器。
func WithKnowledgeConditionedFilter(filter *searchfilter.UniversalFilterCondition) RunOption {
	return func(opts *RunOptions) {
		opts.KnowledgeConditionedFilter = filter
	}
}

// WithMessages sets the caller-supplied conversation history for this run.
// Runner uses this history to auto-seed an empty Session (once) and to
// populate `invocation.Message` via RunWithMessages for compatibility. The
// content processor itself does not read this field; it derives messages from
// Session events (and may fall back to a single `invocation.Message` when the
// Session is empty).
func WithMessages(messages []model.Message) RunOption {
	return func(opts *RunOptions) {
		opts.Messages = messages
	}
}

// WithRequestID sets the request id for the RunOptions.
func WithRequestID(requestID string) RunOption {
	return func(opts *RunOptions) {
		opts.RequestID = requestID
	}
}

// WithModel sets the model for this specific run.
// This allows temporarily switching the model for a single request without
// affecting other requests or the agent's default model configuration.
//
// Example:
//
//	runner.Run(ctx, userID, sessionID, message,
//	    agent.WithModel(customModel),
//	)
func WithModel(m model.Model) RunOption {
	return func(opts *RunOptions) {
		opts.Model = m
	}
}

// WithModelName sets the model name for this specific run.
// The agent will look up the model by name from its registered models.
// This is useful when the agent has multiple models registered via WithModels.
//
// Example:
//
//	runner.Run(ctx, userID, sessionID, message,
//	    agent.WithModelName("gpt-4"),
//	)
func WithModelName(name string) RunOption {
	return func(opts *RunOptions) {
		opts.ModelName = name
	}
}

// WithToolFilter sets a custom tool filter function for this specific run.
// The filter function receives a context and a tool, and returns true if the tool should be included.
//
// This is useful for:
//   - Permission control: restrict tool access based on user roles or runtime conditions
//   - Cost optimization: reduce token usage by limiting tool descriptions
//   - Feature isolation: limit capabilities for specific use cases
//   - Dynamic filtering: filter tools based on runtime state, session data, etc.
//
// Example - Simple name-based filtering:
//
//	runner.Run(ctx, userID, sessionID, message,
//	    agent.WithToolFilter(tool.NewIncludeToolNamesFilter("calculator", "time_tool")),
//	)
//
// Example - Custom logic with runtime state:
//
//	runner.Run(ctx, userID, sessionID, message,
//	    agent.WithToolFilter(func(ctx context.Context, t tool.Tool) bool {
//	        // Access invocation from context if needed
//	        inv, _ := agent.InvocationFromContext(ctx)
//	        userLevel, _ := inv.Session.Get("user_level").(string)
//
//	        // Premium users get all tools
//	        if userLevel == "premium" {
//	            return true
//	        }
//
//	        // Free users only get basic tools
//	        toolName := t.Declaration().Name
//	        return toolName == "calculator" || toolName == "time_tool"
//	    }),
//	)
//
// Note: Framework tools (knowledge_search, transfer_to_agent) are never filtered
// and will always be available regardless of the filter function.
//
// Note: This is a "soft" constraint. Tools should still implement their own
// authorization logic for security.
func WithToolFilter(filter tool.FilterFunc) RunOption {
	return func(opts *RunOptions) {
		opts.ToolFilter = filter
	}
}

// WithA2ARequestOptions sets the A2A request options for the RunOptions.
// These options will be passed to A2A agent's SendMessage and StreamMessage calls.
// This allows passing dynamic HTTP headers or other request-specific options for each run.
func WithA2ARequestOptions(opts ...any) RunOption {
	return func(runOpts *RunOptions) {
		runOpts.A2ARequestOptions = append(runOpts.A2ARequestOptions, opts...)
	}

}

// WithCustomAgentConfigs sets custom agent configurations.
// This allows passing agent-specific configurations at runtime without modifying the agent implementation.
//
// Parameters:
//   - configs: A map where the key is the agent type identifier and the value is the agent-specific config.
//     It's recommended to use the agent's defined RunOptionKey constant as the key and a typed options struct as the value.
//
// Usage:
//
//	// Example: Configure a custom LLM agent using its defined key and options struct
//	import customllm "your.module/agents/customllm"
//
//	runner.Run(ctx, userID, sessionID, message,
//	    agent.WithCustomAgentConfigs(map[string]any{
//	        customllm.RunOptionKey: customllm.RunOptions{
//	            "custom-context": "context",
//	        },
//	    }),
//	)
//
//
//	// In your custom agent implementation, retrieve the config:
//	func (a *CustomLLMAgent) Run(ctx context.Context, inv *agent.Invocation) (<-chan *event.Event, error) {
//	    config := inv.GetCustomAgentConfig(RunOptionKey)
//	    if opts, ok := config.(RunOptions); ok {
//	        client := NewLLMClient(opts.APIKey, opts.Model, opts.Temperature)
//	        // Use the configuration...
//	    }
//	    // ...
//	}
//
// Note:
//   - This function creates a shallow copy of the configs map to prevent external modifications.
//   - The stored configuration should be treated as read-only. Do not modify it after retrieval.
func WithCustomAgentConfigs(configs map[string]any) RunOption {
	return func(opts *RunOptions) {
		if configs == nil {
			opts.CustomAgentConfigs = nil
			return
		}
		// Create a shallow copy to prevent external modifications
		copied := make(map[string]any, len(configs))
		for k, v := range configs {
			copied[k] = v
		}
		opts.CustomAgentConfigs = copied
	}
}

// RunOptions is the options for the Run method.
// RunOptions 是 Run 方法的选项。
type RunOptions struct {
	// RuntimeState contains key-value pairs that will be merged into the initial state
	// for this specific run. This allows callers to pass dynamic parameters
	// (e.g., room ID, user context) without modifying the agent's base initial state.
	// RuntimeState 包含将合并到此特定运行的初始状态的键值对。这允许调用者传递动态参数（例如，房间 ID、
	// 用户上下文），而无需修改代理的基本初始状态
	RuntimeState map[string]any

	// KnowledgeFilter contains metadata key-value pairs for the knowledge filter
	// KnowledgeFilter 包含知识过滤器的元数据键值对
	KnowledgeFilter map[string]any

	// KnowledgeConditionedFilter contains complex condition filter for the knowledge search
	// KnowledgeConditionedFilter 包含用于知识搜索的复杂条件过滤器
	KnowledgeConditionedFilter *searchfilter.UniversalFilterCondition

	// Messages allows callers to provide a full conversation history to Runner.
	// Runner will seed an empty Session with this history automatically and
	// then rely on Session events for subsequent turns. The content processor
	// ignores this field and reads only from Session events (or falls back to
	// `invocation.Message` when no events exist).
	// Messages 允许调用者提供完整的会话历史记录给 Runner。Runner 将自动使用此历史记录初始化一个空 Session，
	// 然后依赖 Session 事件进行后续的回合。内容处理器会忽略此字段，并仅从 Session 事件（或当没有事件时回退到
	// `invocation.Message`）中读取。
	Messages []model.Message

	// RequestID 是请求的请求id。
	RequestID string

	// A2ARequestOptions 包含将传递给的 A2A 客户端请求选项
	// A2A 代理的 SendMessage 和 StreamMessage 调用。这允许调用者通过
	// 每次运行的动态 HTTP 标头或其他特定于请求的选项。
	//
	// 注意：该字段使用任意类型以避免直接依赖 trpc-a2a-go/client 包。
	// 用户应该传递 client.RequestOption 值（例如 client.WithRequestHeader）。
	// a2aagent 包将在运行时验证选项类型。
	A2ARequestOptions []any

	// CustomAgentConfigs 存储自定义 agent 的配置。
	// key：agent 类型，value：特定于agent 的配置。
	CustomAgentConfigs map[string]any

	// Model 是用于此特定运行的模型。
	// 如果设置，它将暂时覆盖此请求的代理默认模型。
	// 这允许按请求进行模型切换，而不会影响其他并发请求。
	Model model.Model

	// ModelName 是用于此特定运行的模型名称。
	// 代理将从其注册的模型中通过名称查找模型。
	// 如果同时设置了 Model 和 ModelName，Model 优先。
	ModelName string

	// ToolFilter 是一个自定义函数，用于过滤本次运行的工具。
	// 如果设置，则只有过滤器返回 true 的工具才可用于模型。
	// 如果为零，则所有注册的工具都将可用（默认行为）。
	//
	// 过滤函数接收：
	// - ctx：带有调用信息的上下文（使用agent.InitationFromContext）
	// - tool: 被过滤的工具
	//
	// 此过滤发生在发送到模型之前的请求准备阶段。
	// 模型只会看到通过过滤器的工具的工具描述。
	//
	// 注意：框架工具（knowledge_search、transfer_to_agent）永远不会被过滤
	// 无论过滤器函数的返回值如何，都将始终包含在内。
	//
	// 示例：
	// agent.WithToolFilter(tool.NewIncludeToolNamesFilter("calculator", "time_tool"))
	// agent.WithToolFilter(func(ctx context.Context, t tool.Tool) bool {
	// 返回 t.Declaration().Name == "calculator"
	// })
	ToolFilter tool.FilterFunc
}

// NewInvocation 创建一个新的 Invocation。
func NewInvocation(invocationOpts ...InvocationOptions) *Invocation {
	inv := &Invocation{
		InvocationID:  uuid.NewString(),
		noticeMu:      &sync.Mutex{},
		noticeChanMap: make(map[string]chan any),
	}

	for _, opt := range invocationOpts {
		opt(inv)
	}

	if inv.Branch == "" {
		inv.Branch = inv.AgentName
	}

	if inv.eventFilterKey == "" && inv.AgentName != "" {
		inv.eventFilterKey = inv.AgentName
	}

	return inv
}

// Clone clone a new invocation
func (inv *Invocation) Clone(invocationOpts ...InvocationOptions) *Invocation {
	if inv == nil {
		return nil
	}
	newInv := &Invocation{
		InvocationID:    uuid.NewString(),
		Session:         inv.Session,
		Message:         inv.Message,
		RunOptions:      inv.RunOptions,
		MemoryService:   inv.MemoryService,
		ArtifactService: inv.ArtifactService,
		noticeMu:        inv.noticeMu,
		noticeChanMap:   inv.noticeChanMap,
		eventFilterKey:  inv.eventFilterKey,
		parent:          inv,
	}

	for _, opt := range invocationOpts {
		opt(newInv)
	}

	if newInv.Branch != "" {
		// seted by WithInvocationBranch
	} else if inv.Branch != "" && newInv.AgentName != "" {
		newInv.Branch = inv.Branch + BranchDelimiter + newInv.AgentName
	} else if newInv.AgentName != "" {
		newInv.Branch = newInv.AgentName
	} else {
		newInv.Branch = inv.Branch
	}

	if newInv.eventFilterKey == "" && newInv.AgentName != "" {
		newInv.eventFilterKey = newInv.AgentName
	}

	return newInv
}

// GetEventFilterKey 获取事件过滤器键。
func (inv *Invocation) GetEventFilterKey() string {
	if inv == nil {
		return ""
	}
	return inv.eventFilterKey
}

// InjectIntoEvent 将调用信息注入到事件中
func InjectIntoEvent(inv *Invocation, e *event.Event) {
	if e == nil || inv == nil {
		return
	}

	e.RequestID = inv.RunOptions.RequestID
	if inv.parent != nil {
		e.ParentInvocationID = inv.parent.InvocationID
	}
	e.InvocationID = inv.InvocationID
	e.Branch = inv.Branch
	e.FilterKey = inv.GetEventFilterKey()
}

// EmitEvent 将调用信息注入事件并将其发送到通道。
func EmitEvent(ctx context.Context, inv *Invocation, ch chan<- *event.Event,
	e *event.Event) error {
	if ch == nil || e == nil {
		return nil
	}
	InjectIntoEvent(inv, e)
	var agentName, requestID string
	if inv != nil {
		agentName = inv.AgentName
		requestID = inv.RunOptions.RequestID
	}
	log.Debugf("[agent.EmitEvent]queue monitoring:RequestID: %s channel capacity: %d, current length: %d, branch: %s, agent name:%s",
		requestID, cap(ch), len(ch), e.Branch, agentName)
	return event.EmitEvent(ctx, ch, e)
}

// GetAppendEventNoticeKey 获取追加事件通知键。
func GetAppendEventNoticeKey(eventID string) string {
	return AppendEventNoticeKeyPrefix + eventID
}

// SetState 在调用状态中设置一个值。
//
// 这是一个适用于调用生命周期的通用键值存储。
// 它可以由回调、中间件或任何调用范围的逻辑使用。
//
// 推荐的键命名约定：
//   - Agent callbacks: "agent:xxx" (e.g., "agent:start_time")
//   - Model callbacks: "model:xxx" (e.g., "model:start_time")
//   - Tool callbacks: "tool:<toolName>:<toolCallID>:xxx" (e.g., "tool:calculator:call_abc123:start_time")
//   - Middleware: "middleware:xxx" (e.g., "middleware:request_id")
//   - Custom logic: "custom:xxx" (e.g., "custom:user_context")
//
// 注意：工具回调应包含工具调用 ID 以支持并发调用。
//
// 例子：
//
//	inv.SetState("agent:start_time", time.Now())
//	inv.SetState("model:start_time", time.Now())
//	inv.SetState("tool:calculator:call_abc123:start_time", time.Now())
//	inv.SetState("middleware:request_id", "req-123")
//	inv.SetState("custom:user_context", userCtx)
func (inv *Invocation) SetState(key string, value any) {
	if inv == nil {
		return
	}
	inv.stateMu.Lock()
	defer inv.stateMu.Unlock()

	if inv.state == nil {
		inv.state = make(map[string]any)
	}
	inv.state[key] = value
}

// GetState 从调用状态检索一个值。
//
// 如果键存在则返回值和 true，否则返回 nil 和 false。
//
// 例子：
//
//	if startTime, ok := inv.GetState("agent:start_time"); ok {
//	    duration := time.Since(startTime.(time.Time))
//	}
//	if startTime, ok := inv.GetState("tool:calculator:call_abc123:start_time"); ok {
//	    duration := time.Since(startTime.(time.Time))
//	}
func (inv *Invocation) GetState(key string) (any, bool) {
	if inv == nil {
		return nil, false
	}
	inv.stateMu.RLock()
	defer inv.stateMu.RUnlock()

	if inv.state == nil {
		return nil, false
	}
	value, ok := inv.state[key]
	return value, ok
}

// DeleteState 从调用状态中删除一个值。
//
// 例子：
//
//	inv.DeleteState("agent:start_time")
//	inv.DeleteState("tool:calculator:call_abc123:start_time")
func (inv *Invocation) DeleteState(key string) {
	if inv == nil {
		return
	}
	inv.stateMu.Lock()
	defer inv.stateMu.Unlock()

	if inv.state != nil {
		delete(inv.state, key)
	}
}

// AddNoticeChannelAndWait 添加通知通道并等待完成
func (inv *Invocation) AddNoticeChannelAndWait(ctx context.Context, key string, timeout time.Duration) error {
	ch := inv.AddNoticeChannel(ctx, key)
	if ch == nil {
		return fmt.Errorf("notice channel create failed for %s", key)
	}
	if timeout == WaitNoticeWithoutTimeout {
		// 没有超时，也许永远等待
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	select {
	case <-ch:
	case <-time.After(timeout):
		return NewWaitNoticeTimeoutError(fmt.Sprintf("Timeout waiting for completion of event %s", key))
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// AddNoticeChannel 添加新的通知通道
func (inv *Invocation) AddNoticeChannel(ctx context.Context, key string) chan any {
	if inv == nil || inv.noticeMu == nil {
		log.Error("noticeMu is uninitialized, please use agent.NewInvocation or Clone method to create Invocation")
		return nil
	}
	inv.noticeMu.Lock()
	defer inv.noticeMu.Unlock()

	if ch, ok := inv.noticeChanMap[key]; ok {
		return ch
	}

	ch := make(chan any)
	if inv.noticeChanMap == nil {
		inv.noticeChanMap = make(map[string]chan any)
	}
	inv.noticeChanMap[key] = ch

	return ch
}

// NotifyCompletion 向等待任务通知完成信号
func (inv *Invocation) NotifyCompletion(ctx context.Context, key string) error {
	if inv == nil || inv.noticeMu == nil {
		log.Error("noticeMu is uninitialized, please use agent.NewInvocation or Clone method to create Invocation")
		return fmt.Errorf("noticeMu is uninitialized, please use agent.NewInvocation or Clone method to create Invocation key:%s", key)
	}
	inv.noticeMu.Lock()
	defer inv.noticeMu.Unlock()

	ch, ok := inv.noticeChanMap[key]
	if !ok {
		return fmt.Errorf("notice channel not found for %s", key)
	}

	close(ch)
	delete(inv.noticeChanMap, key)

	return nil
}

// CleanupNotice 清理所有通知通道
// 应处置通过 NewInvocation 方法创建的“调用”实例
// 完成后防止资源泄漏。
func (inv *Invocation) CleanupNotice(ctx context.Context) {
	if inv == nil || inv.noticeMu == nil {
		log.Error("noticeMu is uninitialized, please use agent.NewInvocation or Clone method to create Invocation")
		return
	}
	inv.noticeMu.Lock()
	defer inv.noticeMu.Unlock()

	for _, ch := range inv.noticeChanMap {
		close(ch)
	}
	inv.noticeChanMap = nil
}

// GetCustomAgentConfig 检索特定自定义 agent 类型的配置。
//
// 参数：
// - agentKey：agent 类型标识符（通常是代理的 RunOptionKey 常量）
//
// 返回：
// - 如果找到则配置值，否则为零
//
// 用法：
//
//	func (a *CustomLLMAgent) Run(ctx context.Context, inv *agent.Invocation) (<-chan *event.Event, error) {
//	    config := inv.GetCustomAgentConfig(RunOptionKey)
//	    if opts, ok := config.(RunOptions); ok {
//	        client := NewLLMClient(opts.APIKey, opts.Model)
//	        // ...
//	    }
//	}
//
// 注意：返回的配置应被视为只读。不要修改它。
func (inv *Invocation) GetCustomAgentConfig(agentKey string) any {
	if inv == nil || inv.RunOptions.CustomAgentConfigs == nil {
		return nil
	}
	return inv.RunOptions.CustomAgentConfigs[agentKey]
}
