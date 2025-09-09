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
	var err error

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

	err = conn.frameRouter.RegisterPendingOperation(pendingOp)
	c.Assert(err, IsNil)

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
	var err error

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
	err = conn.frameRouter.RegisterPendingOperation(pendingOp)
	c.Assert(err, IsNil)
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
	var err error

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

	err = conn.frameRouter.RegisterPendingOperation(pendingOp)
	c.Assert(err, IsNil)

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
	var err error

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

	err = conn.frameRouter.RegisterPendingOperation(pendingOp)
	c.Assert(err, IsNil)

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

			err := conn.frameRouter.RegisterPendingOperation(pendingOp)
			c.Assert(err, IsNil)

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
	var err error

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

	err = conn.frameRouter.RegisterPendingOperation(pendingOp)
	c.Assert(err, IsNil)

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

func (s *FrameRouterSuite) TestTimeoutMonitoringStatus(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	// Give the timeout monitor goroutine a moment to start
	time.Sleep(10 * time.Millisecond)

	// Timeout monitor should be running after creation
	c.Assert(conn.frameRouter.GetTimeoutMonitorStatus(), Equals, true)

	// Test pending operation count
	c.Assert(conn.frameRouter.GetPendingOperationCount(), Equals, 0)

	// Test operation timeouts configuration
	timeouts := conn.frameRouter.GetOperationTimeouts()
	c.Assert(timeouts, NotNil)
	c.Assert(timeouts.Connect, Equals, 30*time.Second)
	c.Assert(timeouts.Send, Equals, 10*time.Second)

	// Test setting operation timeouts
	newTimeouts := &OperationTimeoutConfig{
		Connect:     60 * time.Second,
		Send:        20 * time.Second,
		Subscribe:   15 * time.Second,
		Unsubscribe: 15 * time.Second,
		Disconnect:  20 * time.Second,
		Ack:         10 * time.Second,
	}
	conn.frameRouter.SetOperationTimeouts(newTimeouts)

	updatedTimeouts := conn.frameRouter.GetOperationTimeouts()
	c.Assert(updatedTimeouts.Connect, Equals, 60*time.Second)
	c.Assert(updatedTimeouts.Send, Equals, 20*time.Second)
}

func (s *FrameRouterSuite) TestOperationDoubleRegistration(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	responseCh := make(chan *frame.Frame, 1)
	errorCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pendingOp := &PendingOperation{
		Type:       "send",
		ReceiptID:  "test-receipt-duplicate",
		ResponseCh: responseCh,
		ErrorCh:    errorCh,
		Context:    ctx,
		Cancel:     cancel,
	}

	// First registration should succeed
	err := conn.frameRouter.RegisterPendingOperation(pendingOp)
	c.Assert(err, IsNil)

	// Second registration with same receipt ID should fail
	err = conn.frameRouter.RegisterPendingOperation(pendingOp)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*already exists.*")
}

func (s *FrameRouterSuite) TestOperationValidation(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	// Test nil operation
	err := conn.frameRouter.RegisterPendingOperation(nil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*nil pending operation.*")

	// Test empty receipt ID
	pendingOp := &PendingOperation{
		Type:      "send",
		ReceiptID: "",
	}
	err = conn.frameRouter.RegisterPendingOperation(pendingOp)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*empty receipt ID.*")
}

func (s *FrameRouterSuite) TestTimeoutCleanup(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	// Create an operation with very short timeout
	responseCh := make(chan *frame.Frame, 1)
	errorCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	pendingOp := &PendingOperation{
		Type:       "send",
		ReceiptID:  "test-timeout-cleanup",
		ResponseCh: responseCh,
		ErrorCh:    errorCh,
		Context:    ctx,
		Cancel:     cancel,
	}

	err := conn.frameRouter.RegisterPendingOperation(pendingOp)
	c.Assert(err, IsNil)

	// Wait for timeout
	time.Sleep(50 * time.Millisecond)

	// Manually trigger cleanup
	conn.frameRouter.cleanupExpiredOperations()

	// Operation should be removed
	count := conn.frameRouter.GetPendingOperationCount()
	c.Assert(count, Equals, 0)

	// Error channel should receive timeout error
	select {
	case receivedErr := <-errorCh:
		c.Assert(receivedErr, Equals, context.DeadlineExceeded)
	case <-time.After(100 * time.Millisecond):
		c.Fatal("Expected timeout error not received")
	}
}

// Test the registerAndWaitForReceipt helper method
func (s *FrameRouterSuite) TestRegisterAndWaitForReceipt(c *C) {
	client, _ := NewFakeConn()
	cbioConn := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioConn)

	// Test successful registration
	receiptId := allocateId()
	responseCh, errorCh, cancel, err := conn.registerAndWaitForReceipt("test", receiptId, 100*time.Millisecond)
	defer cancel()

	c.Assert(err, IsNil)
	c.Assert(responseCh, NotNil)
	c.Assert(errorCh, NotNil)

	// Verify operation was registered
	conn.frameRouter.mu.RLock()
	op, exists := conn.frameRouter.pendingOps[receiptId]
	conn.frameRouter.mu.RUnlock()

	c.Assert(exists, Equals, true)
	c.Assert(op.Type, Equals, "test")
	c.Assert(op.ReceiptID, Equals, receiptId)

	// Test duplicate registration
	_, _, cancel2, err := conn.registerAndWaitForReceipt("test", receiptId, 100*time.Millisecond)
	defer cancel2()

	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*already exists.*")
}

// Test the race condition fix by ensuring operations are registered before frames are sent
func (s *FrameRouterSuite) TestOperationRegistrationBeforeSending(c *C) {
	client, server := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	cbioServer := cbio.WrapReadWriteCloser(server)
	conn := NewCallbackConn(cbioClient)

	// Set up a goroutine to simulate a server that responds immediately with a receipt
	go func() {
		reader := frame.NewUnwrapCbioReader(cbioServer)
		writer := frame.NewUnwrapCbioWriter(cbioServer)

		// Read the frame sent by the client
		f, err := reader.ReadSync()
		c.Assert(err, IsNil)
		c.Assert(f.Command, Equals, frame.DISCONNECT)

		// Get the receipt-id
		receiptId := f.Header.Get(frame.Receipt)
		c.Assert(receiptId, Not(Equals), "")

		// Send receipt immediately
		receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
		err = writer.WriteSync(receiptFrame)
		c.Assert(err, IsNil)
	}()

	// Set up a disconnect callback to track completion
	disconnectCompleted := make(chan error, 1)
	conn.Disconnect(func(_ *CallbackConn, err error) {
		disconnectCompleted <- err
	})

	// Wait for disconnect to complete
	select {
	case err := <-disconnectCompleted:
		c.Assert(err, IsNil)
	case <-time.After(1 * time.Second):
		c.Fatal("Disconnect operation timed out")
	}
}

// Test proper handling of late receipts (arriving after timeout)
func (s *FrameRouterSuite) TestLateReceiptHandling(c *C) {
	client, server := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	cbioServer := cbio.WrapReadWriteCloser(server)
	conn := NewCallbackConn(cbioClient)

	// Set a very short timeout for disconnect
	conn.disconnectReceiptTimeout = 50 * time.Millisecond

	// Set up a goroutine to simulate a server that responds with a delay
	go func() {
		reader := frame.NewUnwrapCbioReader(cbioServer)
		writer := frame.NewUnwrapCbioWriter(cbioServer)

		// Read the frame sent by the client
		f, err := reader.ReadSync()
		c.Assert(err, IsNil)
		c.Assert(f.Command, Equals, frame.DISCONNECT)

		// Get the receipt-id
		receiptId := f.Header.Get(frame.Receipt)
		c.Assert(receiptId, Not(Equals), "")

		// Wait longer than the timeout before sending receipt
		time.Sleep(100 * time.Millisecond)

		// Send receipt after timeout
		receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
		err = writer.WriteSync(receiptFrame)
		c.Assert(err, IsNil)
	}()

	// Set up a disconnect callback to track completion
	disconnectCompleted := make(chan error, 1)
	conn.Disconnect(func(_ *CallbackConn, err error) {
		disconnectCompleted <- err
	})

	// Wait for disconnect to complete with timeout error
	select {
	case err := <-disconnectCompleted:
		c.Assert(err, Equals, ErrDisconnectReceiptTimeout)
	case <-time.After(1 * time.Second):
		c.Fatal("Disconnect operation didn't complete")
	}
}

// Test transaction operations with receipt handling
func (s *FrameRouterSuite) TestTransactionOperationsWithReceipt(c *C) {
	client, server := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	cbioServer := cbio.WrapReadWriteCloser(server)
	conn := NewCallbackConn(cbioClient)

	// Set up connection state to Connected for transaction operations
	conn.setState(Connected)

	// Set up a goroutine to simulate a server that responds to transaction operations
	go func() {
		reader := frame.NewUnwrapCbioReader(cbioServer)
		writer := frame.NewUnwrapCbioWriter(cbioServer)

		// Handle BEGIN frame
		beginFrame, err := reader.ReadSync()
		c.Assert(err, IsNil)
		c.Assert(beginFrame.Command, Equals, frame.BEGIN)

		// Handle COMMIT frame with receipt
		commitFrame, err := reader.ReadSync()
		c.Assert(err, IsNil)
		c.Assert(commitFrame.Command, Equals, frame.COMMIT)

		// Get the receipt-id from COMMIT frame
		receiptId := commitFrame.Header.Get(frame.Receipt)
		c.Assert(receiptId, Not(Equals), "")

		// Send receipt immediately
		receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
		err = writer.WriteSync(receiptFrame)
		c.Assert(err, IsNil)
	}()

	// Create transaction
	txCompleted := make(chan error, 1)
	tx, err := conn.Begin(nil)
	c.Assert(err, IsNil)
	c.Assert(tx, NotNil)

	// Commit transaction with callback
	err = tx.Commit(func(_ *CallbackConn, _ *CallbackTransaction, event TransactionEvent, err error) {
		if event == TransactionCommitted {
			txCompleted <- nil
		} else if event == TransactionError {
			txCompleted <- err
		}
	})
	c.Assert(err, IsNil)

	// Wait for transaction to complete
	select {
	case err := <-txCompleted:
		c.Assert(err, IsNil)
	case <-time.After(1 * time.Second):
		c.Fatal("Transaction commit operation timed out")
	}
}
