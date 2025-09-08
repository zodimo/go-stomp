# Epic: Implement Persistent Frame Reader for CallbackConn

## Problem Statement

The current `CallbackConn` implementation has a fundamental architectural flaw that prevents reliable bidirectional STOMP communication. Each operation (Connect, Send, Subscribe, Unsubscribe, Disconnect) creates its own temporary frame reader, leading to race conditions and missed frames.

### Current Architecture Issues

1. **Multiple Competing Readers**: Each operation creates a new `frame.Reader`, causing race conditions when multiple operations try to read from the same connection simultaneously.

2. **Missed RECEIPT Frames**: Server responses (like RECEIPT frames) may be consumed by the wrong reader or missed entirely, causing operations to timeout even when the server responds correctly.

3. **No Frame Routing**: There's no centralized mechanism to route incoming frames to the appropriate handlers based on frame type, receipt IDs, or subscription IDs.

4. **Connection State Inconsistency**: Without a persistent reader, the connection state can become inconsistent with the actual server state.

## Solution Overview

Implement a **persistent background frame reader** that continuously reads frames from the connection and routes them to appropriate handlers, similar to how the existing `Conn` implementation works.

## Epic Scope

### Phase 1: Core Infrastructure (High Priority)

#### Story 2.1: Implement Background Frame Reader
**Acceptance Criteria:**
- [ ] Create a persistent goroutine that continuously reads frames from the connection
- [ ] Implement proper error handling and connection cleanup
- [ ] Ensure the reader starts when connection is established and stops when disconnected
- [ ] Add comprehensive logging for debugging

**Technical Details:**
- Create `startFrameReader()` method that launches background goroutine
- Implement `stopFrameReader()` method for clean shutdown
- Handle connection errors and notify callbacks appropriately

#### Story 2.2: Frame Routing System
**Acceptance Criteria:**
- [ ] Route RECEIPT frames to pending operations based on receipt-id
- [ ] Route MESSAGE frames to appropriate subscription handlers
- [ ] Route ERROR frames to error callbacks
- [ ] Route CONNECTED frames to connection callbacks
- [ ] Handle unknown/unexpected frames gracefully

**Technical Details:**
- Implement frame dispatcher with routing logic
- Create pending operations registry keyed by receipt-id
- Maintain subscription registry for message routing

#### Story 2.3: Operation Synchronization
**Acceptance Criteria:**
- [ ] Replace individual readers with operation registration system
- [ ] Implement timeout handling for pending operations
- [ ] Ensure thread-safe access to pending operations map
- [ ] Clean up expired/completed operations

**Technical Details:**
- Create `PendingOperation` struct to track waiting operations
- Implement operation timeout with context cancellation
- Add mutex protection for concurrent access

### Phase 3: Operation Implementation (Medium Priority)

#### Story 3.1: Refactor Connect Operation
**Acceptance Criteria:**
- [ ] Remove temporary reader from `performConnect`
- [ ] Register connect operation with frame router
- [ ] Handle CONNECTED/ERROR responses through router
- [ ] Maintain backward compatibility with existing API

#### Story 3.2: Refactor Send Operations
**Acceptance Criteria:**
- [ ] Remove temporary readers from send operations
- [ ] Register send operations with receipt tracking
- [ ] Handle RECEIPT/ERROR responses through router
- [ ] Support both receipt and non-receipt send modes

#### Story 3.3: Refactor Subscription Operations
**Acceptance Criteria:**
- [ ] Remove temporary readers from subscribe/unsubscribe
- [ ] Register subscription operations with receipt tracking
- [ ] Route MESSAGE frames to correct subscription handlers
- [ ] Handle subscription lifecycle properly

#### Story 3.4: Refactor Transaction Operations
**Acceptance Criteria:**
- [ ] Update transaction operations to use frame router
- [ ] Handle transaction-related frames properly
- [ ] Maintain transaction state consistency

#### Story 2.5: Refactor Disconnect Operation
**Acceptance Criteria:**
- [ ] Remove temporary reader from disconnect operation
- [ ] Ensure proper cleanup of frame reader on disconnect
- [ ] Handle RECEIPT response for disconnect

### Phase 4: Advanced Features (Low Priority)

#### Story 4.1: Heart-beat Support
**Acceptance Criteria:**
- [ ] Implement heart-beat frame handling in frame router
- [ ] Support both sending and receiving heart-beats
- [ ] Handle heart-beat timeout detection
- [ ] Integrate with existing heart-beat callback system

#### Story 4.2: Message Acknowledgment
**Acceptance Criteria:**
- [ ] Route ACK/NACK frames through frame router
- [ ] Handle ACK receipt tracking if requested
- [ ] Support different acknowledgment modes

#### Story 4.3: Enhanced Error Recovery
**Acceptance Criteria:**
- [ ] Implement connection recovery mechanisms
- [ ] Handle partial frame reads and connection interruptions
- [ ] Provide detailed error reporting through callbacks

### Phase 5: Testing & Validation (Ongoing)

#### Story 5.1: Unit Test Coverage
**Acceptance Criteria:**
- [ ] Create comprehensive unit tests for frame router
- [ ] Test all frame routing scenarios
- [ ] Test concurrent operation handling
- [ ] Test error conditions and edge cases

#### Story 5.2: Integration Test Updates
**Acceptance Criteria:**
- [ ] Update existing integration tests to work with new architecture
- [ ] Create new integration tests for complex scenarios
- [ ] Ensure all timeout issues are resolved
- [ ] Validate performance under load

#### Story 5.3: Backward Compatibility
**Acceptance Criteria:**
- [ ] Ensure existing API remains unchanged
- [ ] Validate that all callback signatures remain compatible
- [ ] Test migration path for existing code

## Technical Architecture

### Core Components

```go
// Frame router that handles incoming frames
type FrameRouter struct {
    pendingOps    map[string]*PendingOperation
    subscriptions map[string]*CallbackSubscription
    mu           sync.RWMutex
    stopCh       chan struct{}
}

// Represents an operation waiting for a response
type PendingOperation struct {
    Type      OperationType
    ReceiptID string
    Callback  interface{}
    Context   context.Context
    Cancel    context.CancelFunc
}

// Updated CallbackConn with frame router
type CallbackConn struct {
    // ... existing fields ...
    frameRouter   *FrameRouter
    readerRunning bool
    readerDone    chan struct{}
}
```

### Key Methods

```go
func (c *CallbackConn) startFrameReader()
func (c *CallbackConn) stopFrameReader()
func (c *CallbackConn) registerOperation(op *PendingOperation)
func (c *CallbackConn) completeOperation(receiptID string, result interface{})
func (r *FrameRouter) routeFrame(frame *frame.Frame)
```

## Success Metrics

- [ ] All integration tests pass without timeouts
- [ ] No race conditions in concurrent operations
- [ ] RECEIPT frames are properly received and handled
- [ ] Connection state remains consistent
- [ ] Performance is comparable to or better than current implementation
- [ ] Memory usage is reasonable (no memory leaks from pending operations)

## Risks & Mitigations

### Risk: Breaking Existing API
**Mitigation**: Maintain strict backward compatibility, extensive testing

### Risk: Performance Degradation
**Mitigation**: Benchmark against current implementation, optimize hot paths

### Risk: Increased Complexity
**Mitigation**: Clear documentation, comprehensive unit tests, gradual rollout

### Risk: Race Conditions in New Code
**Mitigation**: Careful synchronization design, race condition testing

## Dependencies

- No external dependencies required
- May need to refactor some frame handling utilities
- Integration with existing callback system

## Timeline Estimate

- **Phase 1**: 2-3 weeks (Core Infrastructure)
- **Phase 2**: 3-4 weeks (Operation Implementation)
- **Phase 3**: 1-2 weeks (Advanced Features)
- **Phase 4**: Ongoing (Testing & Validation)

**Total Estimated Time**: 6-9 weeks

## Definition of Done

- [ ] All unit tests pass
- [ ] All integration tests pass without timeouts
- [ ] Code review completed
- [ ] Documentation updated
- [ ] Performance benchmarks meet requirements
- [ ] No regressions in existing functionality
- [ ] Memory leak testing completed
