<!-- Powered by BMAD™ Core -->

# Callback-Style STOMP Client - Brownfield Enhancement

## Epic Goal

Implement a callback-style STOMP client using github.com/zodimo/go-netkit@v0.2.0 cbio interfaces to provide asynchronous, event-driven communication capabilities while maintaining full compatibility with existing synchronous client functionality.

## Epic Description

**Existing System Context:**

- Current relevant functionality: Synchronous STOMP client library with connection-based operations using `io.ReadWriteCloser`
- Technology stack: Go 1.15+, STOMP protocol versions 1.0-1.2, frame-based messaging
- Integration points: New callback client will coexist with existing `Conn` implementation, sharing frame and protocol handling

**Enhancement Details:**

- What's being added/changed: New callback-style client implementation using go-netkit cbio interfaces for asynchronous I/O operations
- How it integrates: Separate client implementation that reuses existing frame parsing, protocol negotiation, and STOMP specification compliance
- Success criteria: 
  - Callback client handles all STOMP operations asynchronously
  - Full protocol compatibility maintained (versions 1.0, 1.1, 1.2)
  - Event-driven message handling with registered callbacks
  - Existing synchronous client remains unchanged and fully functional

## Stories

### Story 1: Implement cbio-based connection wrapper and basic STOMP protocol handling

**Description:** Create the foundation for callback-style client with connection management and basic protocol operations.

**Acceptance Criteria:**
- [ ] Add go-netkit@v0.2.0 dependency to go.mod
- [ ] Implement `CallbackConn` struct using cbio interfaces
- [ ] Support CONNECT/CONNECTED frame exchange with callbacks
- [ ] Support DISCONNECT/RECEIPT frame exchange with callbacks
- [ ] Implement protocol version negotiation (1.0, 1.1, 1.2)
- [ ] Add connection state management with event callbacks
- [ ] Create basic error handling with callback mechanisms
- [ ] All existing tests continue to pass

### Story 2: Add callback-based message sending and subscription management

**Description:** Implement message publishing and subscription functionality with event-driven handlers.

**Acceptance Criteria:**
- [ ] Implement SEND frame handling with success/error callbacks
- [ ] Support SUBSCRIBE/UNSUBSCRIBE operations with callback confirmation
- [ ] Add MESSAGE frame processing with registered message handlers
- [ ] Implement subscription management with callback notifications
- [ ] Support receipt handling for reliable message delivery
- [ ] Add message acknowledgment (ACK/NACK) with callbacks
- [ ] Create subscription lifecycle event handlers
- [ ] All existing tests continue to pass

### Story 3: Integrate heart-beating, transaction support, and error handling

**Description:** Complete the callback client with advanced STOMP features and comprehensive error handling.

**Acceptance Criteria:**
- [ ] Implement heart-beat negotiation and monitoring with callbacks
- [ ] Add BEGIN/COMMIT/ABORT transaction support with callbacks
- [ ] Support transactional message sending and acknowledgment
- [ ] Implement comprehensive error handling and recovery callbacks
- [ ] Add connection health monitoring with status callbacks
- [ ] Support graceful shutdown with cleanup callbacks
- [ ] Create example usage documentation
- [ ] Add integration tests for callback client
- [ ] All existing tests continue to pass

## Compatibility Requirements

- [ ] Existing APIs remain unchanged
- [ ] Database schema changes are backward compatible
- [ ] UI changes follow existing patterns
- [ ] Performance impact is minimal

## Risk Mitigation

- **Primary Risk:** Complexity of maintaining two different I/O paradigms (sync vs callback)
- **Mitigation:** Reuse existing frame handling, protocol logic, and STOMP specification compliance; maintain clear separation between sync and async implementations
- **Rollback Plan:** Remove new callback client files and revert go.mod dependencies if issues arise

## Definition of Done

- [ ] All stories completed with acceptance criteria met
- [ ] Existing functionality verified through testing
- [ ] Integration points working correctly
- [ ] Documentation updated appropriately
- [ ] No regression in existing features

## Technical Implementation Notes

### Shared Components
The callback client will reuse these existing components:
- `frame` package - Frame parsing, encoding, and protocol handling
- Protocol version negotiation logic
- STOMP specification compliance mechanisms
- Header processing and validation

### New Components
- `CallbackConn` - Main callback-based connection interface
- Event handler registration system
- Asynchronous operation management
- cbio interface implementations

### Integration Points
- Frame processing pipeline remains shared
- Protocol negotiation reuses existing logic
- Error types and handling patterns consistent with existing client
- Configuration options follow established patterns

## Dependencies

- **External:** github.com/zodimo/go-netkit@v0.2.0 (cbio interfaces)
- **Internal:** Existing frame package, protocol handling, STOMP specification support

## Story Manager Handoff

**Story Manager Instructions:**

"Please develop detailed user stories for this brownfield epic. Key considerations:

- This is an enhancement to an existing system running Go 1.15+ with synchronous STOMP client implementation
- Integration points: Shared frame parsing (`github.com/go-stomp/stomp/v3/frame`), protocol negotiation, and STOMP specification handling
- Existing patterns to follow: Frame-based messaging, version negotiation, heart-beating, subscription management, transaction support
- Critical compatibility requirements: 
  - All existing synchronous client APIs must remain unchanged
  - New callback client must support all STOMP protocol versions (1.0, 1.1, 1.2)
  - Shared components (frame handling, protocol logic) must not be modified in breaking ways
- Each story must include verification that existing functionality remains intact

The epic should maintain system integrity while delivering asynchronous, callback-based STOMP client functionality using go-netkit cbio interfaces."

---

## Epic Metadata

**Epic Status:** Ready for Story Development  
**Epic Owner:** Development Team  
**Priority:** Medium  
**Estimated Effort:** 3 Stories, ~2-3 weeks  
**Dependencies:** github.com/zodimo/go-netkit@v0.2.0
