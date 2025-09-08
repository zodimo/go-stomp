package stomp

import (
	"context"
	"sync"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
	"github.com/zodimo/go-netkit/cbio"
	. "gopkg.in/check.v1"
)

type FrameRouterSuite struct{}

var _ = Suite(&FrameRouterSuite{})

func (s *FrameRouterSuite) TestFrameRouterCreation(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	c.Assert(conn.frameRouter, NotNil)
	c.Assert(conn.frameRouter.conn, Equals, conn)
	c.Assert(conn.frameRouter.pendingOps, NotNil)
	c.Assert(conn.frameRouter.subscriptions, NotNil)
}

func (s *FrameRouterSuite) TestRegisterPendingOperation(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	responseCh := make(chan *frame.Frame, 1)
	errorCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pendingOp := &PendingOperation{
		Type:       "send",
		ReceiptID:  "test-receipt-123",
		ResponseCh: responseCh,
		ErrorCh:    errorCh,
		Context:    ctx,
		Cancel:     cancel,
	}

	conn.frameRouter.RegisterPendingOperation(pendingOp)

	// Verify operation was registered
	conn.frameRouter.mu.RLock()
	registeredOp, exists := conn.frameRouter.pendingOps["test-receipt-123"]
	conn.frameRouter.mu.RUnlock()

	c.Assert(exists, Equals, true)
	c.Assert(registeredOp.Type, Equals, "send")
	c.Assert(registeredOp.ReceiptID, Equals, "test-receipt-123")
}

func (s *FrameRouterSuite) TestUnregisterPendingOperation(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	responseCh := make(chan *frame.Frame, 1)
	errorCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pendingOp := &PendingOperation{
		Type:       "send",
		ReceiptID:  "test-receipt-123",
		ResponseCh: responseCh,
		ErrorCh:    errorCh,
		Context:    ctx,
		Cancel:     cancel,
	}

	// Register then unregister
	conn.frameRouter.RegisterPendingOperation(pendingOp)
	conn.frameRouter.UnregisterPendingOperation("test-receipt-123")

	// Verify operation was unregistered
	conn.frameRouter.mu.RLock()
	_, exists := conn.frameRouter.pendingOps["test-receipt-123"]
	conn.frameRouter.mu.RUnlock()

	c.Assert(exists, Equals, false)
}

func (s *FrameRouterSuite) TestRouteReceiptFrameSuccess(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	responseCh := make(chan *frame.Frame, 1)
	errorCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pendingOp := &PendingOperation{
		Type:       "send",
		ReceiptID:  "test-receipt-123",
		ResponseCh: responseCh,
		ErrorCh:    errorCh,
		Context:    ctx,
		Cancel:     cancel,
	}

	conn.frameRouter.RegisterPendingOperation(pendingOp)

	// Create RECEIPT frame
	receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, "test-receipt-123")

	// Route the frame
	conn.frameRouter.RouteFrame(receiptFrame)

	// Verify receipt was delivered
	select {
	case receivedFrame := <-responseCh:
		c.Assert(receivedFrame.Command, Equals, frame.RECEIPT)
		c.Assert(receivedFrame.Header.Get(frame.ReceiptId), Equals, "test-receipt-123")
	case <-time.After(100 * time.Millisecond):
		c.Fatal("Receipt frame not delivered")
	}

	// Verify operation was unregistered
	conn.frameRouter.mu.RLock()
	_, exists := conn.frameRouter.pendingOps["test-receipt-123"]
	conn.frameRouter.mu.RUnlock()

	c.Assert(exists, Equals, false)
}

func (s *FrameRouterSuite) TestRouteReceiptFrameUnknownReceiptID(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	// Create RECEIPT frame for unknown receipt ID
	receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, "unknown-receipt-456")

	// Route the frame (should not panic, just log warning)
	conn.frameRouter.RouteFrame(receiptFrame)

	// Test passes if no panic occurs
	c.Assert(true, Equals, true)
}

func (s *FrameRouterSuite) TestRouteReceiptFrameNoReceiptID(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	// Create RECEIPT frame without receipt-id header
	receiptFrame := frame.New(frame.RECEIPT)

	// Route the frame (should not panic, just log error)
	conn.frameRouter.RouteFrame(receiptFrame)

	// Test passes if no panic occurs
	c.Assert(true, Equals, true)
}

func (s *FrameRouterSuite) TestRouteErrorFrameWithReceiptID(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	responseCh := make(chan *frame.Frame, 1)
	errorCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pendingOp := &PendingOperation{
		Type:       "send",
		ReceiptID:  "test-receipt-123",
		ResponseCh: responseCh,
		ErrorCh:    errorCh,
		Context:    ctx,
		Cancel:     cancel,
	}

	conn.frameRouter.RegisterPendingOperation(pendingOp)

	// Create ERROR frame with receipt-id
	errorFrame := frame.New(frame.ERROR, frame.ReceiptId, "test-receipt-123", frame.Message, "Test error")

	// Route the frame
	conn.frameRouter.RouteFrame(errorFrame)

	// Verify error was delivered to pending operation
	select {
	case receivedError := <-errorCh:
		c.Assert(receivedError, NotNil)
		c.Assert(receivedError.Error(), Matches, ".*Test error.*")
	case <-time.After(100 * time.Millisecond):
		c.Fatal("Error not delivered to pending operation")
	}

	// Verify operation was unregistered
	conn.frameRouter.mu.RLock()
	_, exists := conn.frameRouter.pendingOps["test-receipt-123"]
	conn.frameRouter.mu.RUnlock()

	c.Assert(exists, Equals, false)
}

func (s *FrameRouterSuite) TestRouteMessageFrame(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	// Create MESSAGE frame
	messageFrame := frame.New(frame.MESSAGE, frame.Subscription, "sub-123", frame.Destination, "/topic/test")

	// Route the frame (should delegate to handleMessageFrame)
	conn.frameRouter.RouteFrame(messageFrame)

	// Test passes if no panic occurs
	c.Assert(true, Equals, true)
}

func (s *FrameRouterSuite) TestRouteUnknownFrame(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	// Create frame with unknown command
	unknownFrame := frame.New("UNKNOWN_COMMAND")

	// Route the frame (should log warning)
	conn.frameRouter.RouteFrame(unknownFrame)

	// Test passes if no panic occurs
	c.Assert(true, Equals, true)
}

func (s *FrameRouterSuite) TestConcurrentPendingOperations(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	const numOperations = 10
	var wg sync.WaitGroup
	wg.Add(numOperations)

	// Register multiple pending operations concurrently
	for i := 0; i < numOperations; i++ {
		go func(id int) {
			defer wg.Done()

			responseCh := make(chan *frame.Frame, 1)
			errorCh := make(chan error, 1)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			receiptID := allocateId()
			pendingOp := &PendingOperation{
				Type:       "send",
				ReceiptID:  receiptID,
				ResponseCh: responseCh,
				ErrorCh:    errorCh,
				Context:    ctx,
				Cancel:     cancel,
			}

			conn.frameRouter.RegisterPendingOperation(pendingOp)

			// Create and route RECEIPT frame
			receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptID)
			conn.frameRouter.RouteFrame(receiptFrame)

			// Verify receipt was delivered
			select {
			case receivedFrame := <-responseCh:
				c.Assert(receivedFrame.Command, Equals, frame.RECEIPT)
				c.Assert(receivedFrame.Header.Get(frame.ReceiptId), Equals, receiptID)
			case <-time.After(100 * time.Millisecond):
				c.Fatalf("Receipt frame not delivered for operation %d", id)
			}
		}(i)
	}

	wg.Wait()

	// Verify all operations were cleaned up
	conn.frameRouter.mu.RLock()
	pendingCount := len(conn.frameRouter.pendingOps)
	conn.frameRouter.mu.RUnlock()

	c.Assert(pendingCount, Equals, 0)
}

func (s *FrameRouterSuite) TestFrameRouterStop(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	responseCh := make(chan *frame.Frame, 1)
	errorCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pendingOp := &PendingOperation{
		Type:       "send",
		ReceiptID:  "test-receipt-123",
		ResponseCh: responseCh,
		ErrorCh:    errorCh,
		Context:    ctx,
		Cancel:     cancel,
	}

	conn.frameRouter.RegisterPendingOperation(pendingOp)

	// Stop the frame router
	conn.frameRouter.Stop()

	// Verify error was sent to pending operation
	select {
	case receivedError := <-errorCh:
		c.Assert(receivedError, Equals, ErrClosedUnexpectedly)
	case <-time.After(100 * time.Millisecond):
		c.Fatal("Error not delivered to pending operation")
	}

	// Verify all operations were cleared
	conn.frameRouter.mu.RLock()
	pendingCount := len(conn.frameRouter.pendingOps)
	subscriptionCount := len(conn.frameRouter.subscriptions)
	conn.frameRouter.mu.RUnlock()

	c.Assert(pendingCount, Equals, 0)
	c.Assert(subscriptionCount, Equals, 0)
}

func (s *FrameRouterSuite) TestSubscriptionRegistration(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	subscription := &CallbackSubscription{
		id:          "sub-123",
		destination: "/topic/test",
	}

	// Register subscription
	conn.frameRouter.RegisterSubscription("sub-123", subscription)

	// Verify subscription was registered
	conn.frameRouter.mu.RLock()
	registeredSub, exists := conn.frameRouter.subscriptions["sub-123"]
	conn.frameRouter.mu.RUnlock()

	c.Assert(exists, Equals, true)
	c.Assert(registeredSub.Id(), Equals, "sub-123")
	c.Assert(registeredSub.destination, Equals, "/topic/test")

	// Unregister subscription
	conn.frameRouter.UnregisterSubscription("sub-123")

	// Verify subscription was unregistered
	conn.frameRouter.mu.RLock()
	_, exists = conn.frameRouter.subscriptions["sub-123"]
	conn.frameRouter.mu.RUnlock()

	c.Assert(exists, Equals, false)
}
