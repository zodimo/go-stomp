package stomp

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
	"github.com/zodimo/go-netkit/cbio"
)

// newTestConnections creates a pair of connected pipes for testing
func newTestConnections() (server, client net.Conn) {
	serverConn, clientConn := net.Pipe()
	return serverConn, clientConn
}

func TestCallbackConn_FullIntegration(t *testing.T) {
	// Skip if we don't have a test server available
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create test server connection
	serverConn, clientConn := newTestConnections()
	defer serverConn.Close()
	defer clientConn.Close()

	// Create callback connection
	callbackConn := NewCallbackConn(cbio.WrapReadWriteCloser(clientConn))

	// Test connection lifecycle
	t.Run("ConnectionLifecycle", func(t *testing.T) {
		testConnectionLifecycle(t, callbackConn, serverConn)
	})
}

func testConnectionLifecycle(t *testing.T, callbackConn *CallbackConn, serverConn net.Conn) {
	var wg sync.WaitGroup
	var connectSuccess bool

	// Set up callbacks
	callbackConn.SetStateChangeCallback(func(conn *CallbackConn, oldState, newState ConnectionState) {
		t.Logf("State change: %s -> %s", oldState, newState)
	})

	callbackConn.SetHealthStatusCallback(func(conn *CallbackConn, oldStatus, newStatus ConnectionHealth) {
		t.Logf("Health change: %s -> %s", oldStatus, newStatus)
	})

	callbackConn.SetHeartBeatCallback(func(conn *CallbackConn, event HeartBeatEvent, err error) {
		if err != nil {
			t.Logf("Heart-beat error (%s): %v", event, err)
		} else {
			t.Logf("Heart-beat event: %s", event)
		}
	})

	// Start server response handler
	go handleServerResponses(serverConn)

	// Test connection
	wg.Add(1)
	err := callbackConn.Connect(func(conn *CallbackConn, session, server string, version Version) {
		t.Logf("Connected: session=%s, server=%s, version=%s", session, server, version)
		connectSuccess = true
		wg.Done()
	})

	if err != nil {
		t.Fatalf("Failed to initiate connection: %v", err)
	}

	// Wait for connection with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if !connectSuccess {
			t.Fatal("Connection callback was not called successfully")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connection timeout")
	}

	// Verify connection state
	if callbackConn.GetState() != Connected {
		t.Errorf("Expected state Connected, got %s", callbackConn.GetState())
	}

	if callbackConn.GetHealthStatus() != HealthHealthy {
		t.Errorf("Expected health HealthHealthy, got %s", callbackConn.GetHealthStatus())
	}

	// Test messaging
	testMessaging(t, callbackConn)

	// Test transactions
	testTransactions(t, callbackConn)

	// Test subscriptions
	testSubscriptions(t, callbackConn)

	// Test statistics
	testStatistics(t, callbackConn)

	// Test disconnection
	err = callbackConn.Disconnect(func(conn *CallbackConn, err error) {
		t.Logf("Disconnected: %v", err)
	})

	if err != nil {
		t.Fatalf("Failed to initiate disconnection: %v", err)
	}

	// Wait for disconnection to complete (give it a moment)
	time.Sleep(500 * time.Millisecond)

	// Verify disconnected state
	if callbackConn.GetState() != Disconnected {
		t.Errorf("Expected state Disconnected, got %s", callbackConn.GetState())
	}

	if callbackConn.GetHealthStatus() != HealthDisconnected {
		t.Errorf("Expected health HealthDisconnected, got %s", callbackConn.GetHealthStatus())
	}
}

func testMessaging(t *testing.T, conn *CallbackConn) {
	t.Log("Testing messaging...")

	var sendSuccess bool
	var wg sync.WaitGroup

	wg.Add(1)
	err := conn.Send("/queue/test", "text/plain", []byte("test message"),
		func(conn *CallbackConn, destination string, err error) {
			if err != nil {
				t.Errorf("Send callback error: %v", err)
			} else {
				t.Logf("Message sent to %s", destination)
				sendSuccess = true
			}
			wg.Done()
		})

	if err != nil {
		t.Errorf("Failed to send message: %v", err)
		return
	}

	// Wait for send completion
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if !sendSuccess {
			t.Error("Send was not successful")
		}
	case <-time.After(2 * time.Second):
		t.Error("Send timeout")
	}
}

func testTransactions(t *testing.T, conn *CallbackConn) {
	t.Log("Testing transactions...")

	// Test with longer timeout to ensure we catch actual issues, not just timing
	testTimeout := 5 * time.Second

	// Test Begin Transaction
	t.Run("BeginTransaction", func(t *testing.T) {
		var txBeginSuccess bool
		var wg sync.WaitGroup

		wg.Add(1)
		tx, err := conn.Begin(func(conn *CallbackConn, tx *CallbackTransaction, event TransactionEvent, err error) {
			if err != nil {
				t.Errorf("Transaction callback error (%s): %v", event, err)
			} else {
				t.Logf("Transaction event: %s (ID: %s)", event, tx.Id())
				if event == TransactionBegan {
					txBeginSuccess = true
					wg.Done()
				}
			}
		})

		if err != nil {
			t.Errorf("Failed to begin transaction: %v", err)
			return
		}

		// Wait for begin callback
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			if !txBeginSuccess {
				t.Error("Transaction begin was not successful")
			}
		case <-time.After(testTimeout):
			t.Error("Transaction begin timeout")
		}

		// Verify transaction state
		if tx.State() != TxStateActive {
			t.Errorf("Expected transaction state TxStateActive, got %s", tx.State())
		}

		// Cleanup - abort the transaction
		if tx.State() == TxStateActive {
			tx.Abort(nil)
		}
	})

	// Test Transaction Commit
	t.Run("TransactionCommit", func(t *testing.T) {
		var txBeginSuccess, txCommitSuccess, txSendSuccess bool
		var wg sync.WaitGroup

		// Begin transaction
		wg.Add(1)
		tx, err := conn.Begin(func(conn *CallbackConn, tx *CallbackTransaction, event TransactionEvent, err error) {
			if err != nil {
				t.Errorf("Transaction callback error (%s): %v", event, err)
			} else {
				t.Logf("Transaction event: %s (ID: %s)", event, tx.Id())
				if event == TransactionBegan {
					txBeginSuccess = true
					wg.Done()
				}
			}
		})

		if err != nil {
			t.Errorf("Failed to begin transaction: %v", err)
			return
		}

		// Wait for begin callback
		beginDone := make(chan struct{})
		go func() {
			wg.Wait()
			close(beginDone)
		}()

		select {
		case <-beginDone:
			if !txBeginSuccess {
				t.Error("Transaction begin was not successful")
				return
			}
		case <-time.After(testTimeout):
			t.Error("Transaction begin timeout")
			return
		}

		// Test transactional send
		wg.Add(1)
		err = tx.Send("/queue/tx-test", "application/json", []byte(`{"test": true}`),
			func(conn *CallbackConn, destination string, err error) {
				if err != nil {
					t.Errorf("Transactional send error: %v", err)
				} else {
					t.Logf("Transactional message sent to %s", destination)
					txSendSuccess = true
				}
				wg.Done()
			})

		if err != nil {
			t.Errorf("Failed to send transactional message: %v", err)
			return
		}

		// Wait for send completion
		sendDone := make(chan struct{})
		go func() {
			wg.Wait()
			close(sendDone)
		}()

		select {
		case <-sendDone:
			if !txSendSuccess {
				t.Error("Transactional send was not successful")
				return
			}
		case <-time.After(testTimeout):
			t.Error("Transactional send timeout")
			return
		}

		// Test transaction commit
		wg.Add(1)
		err = tx.Commit(func(conn *CallbackConn, tx *CallbackTransaction, event TransactionEvent, err error) {
			if err != nil {
				t.Errorf("Transaction commit error: %v", err)
			} else {
				t.Logf("Transaction committed: %s", tx.Id())
				if event == TransactionCommitted {
					txCommitSuccess = true
					wg.Done()
				}
			}
		})

		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
			return
		}

		// Wait for commit completion
		commitDone := make(chan struct{})
		go func() {
			wg.Wait()
			close(commitDone)
		}()

		select {
		case <-commitDone:
			if !txCommitSuccess {
				t.Error("Transaction commit was not successful")
			}
		case <-time.After(testTimeout):
			t.Error("Transaction commit timeout")
		}

		// Verify transaction state
		if tx.State() != TxStateCommitted {
			t.Errorf("Expected transaction state TxStateCommitted, got %s", tx.State())
		}
	})

	// Test Transaction Abort
	t.Run("TransactionAbort", func(t *testing.T) {
		var txBeginSuccess, txAbortSuccess bool
		var wg sync.WaitGroup

		// Begin transaction
		wg.Add(1)
		tx, err := conn.Begin(func(conn *CallbackConn, tx *CallbackTransaction, event TransactionEvent, err error) {
			if err != nil {
				t.Errorf("Transaction callback error (%s): %v", event, err)
			} else {
				t.Logf("Transaction event: %s (ID: %s)", event, tx.Id())
				if event == TransactionBegan {
					txBeginSuccess = true
					wg.Done()
				}
			}
		})

		if err != nil {
			t.Errorf("Failed to begin transaction: %v", err)
			return
		}

		// Wait for begin callback
		beginDone := make(chan struct{})
		go func() {
			wg.Wait()
			close(beginDone)
		}()

		select {
		case <-beginDone:
			if !txBeginSuccess {
				t.Error("Transaction begin was not successful")
				return
			}
		case <-time.After(testTimeout):
			t.Error("Transaction begin timeout")
			return
		}

		// Test transaction abort
		wg.Add(1)
		err = tx.Abort(func(conn *CallbackConn, tx *CallbackTransaction, event TransactionEvent, err error) {
			if err != nil {
				t.Errorf("Transaction abort error: %v", err)
			} else {
				t.Logf("Transaction aborted: %s", tx.Id())
				if event == TransactionAborted {
					txAbortSuccess = true
					wg.Done()
				}
			}
		})

		if err != nil {
			t.Errorf("Failed to abort transaction: %v", err)
			return
		}

		// Wait for abort completion
		abortDone := make(chan struct{})
		go func() {
			wg.Wait()
			close(abortDone)
		}()

		select {
		case <-abortDone:
			if !txAbortSuccess {
				t.Error("Transaction abort was not successful")
			}
		case <-time.After(testTimeout):
			t.Error("Transaction abort timeout")
		}

		// Verify transaction state
		if tx.State() != TxStateAborted {
			t.Errorf("Expected transaction state TxStateAborted, got %s", tx.State())
		}
	})
}

func testSubscriptions(t *testing.T, conn *CallbackConn) {
	t.Log("Testing subscriptions...")

	var subscribeSuccess, unsubscribeSuccess bool
	var wg sync.WaitGroup

	// Test subscription
	wg.Add(1)
	sub, err := conn.Subscribe("/queue/sub-test", AckClient,
		func(conn *CallbackConn, message *CallbackMessage) {
			t.Logf("Received message: %s", string(message.Body))
		},
		func(conn *CallbackConn, subscription *CallbackSubscription, event SubscriptionEvent, err error) {
			if err != nil {
				t.Errorf("Subscription callback error (%s): %v", event, err)
			} else {
				t.Logf("Subscription event: %s (ID: %s)", event, subscription.Id())
				if event == SubscriptionCreated {
					subscribeSuccess = true
					wg.Done() // Only call Done() for SubscriptionCreated
				} else if event == SubscriptionUnsubscribed {
					unsubscribeSuccess = true
					wg.Done() // Only call Done() for SubscriptionUnsubscribed
				}
				// Note: SubscriptionActive events are logged but don't trigger wg.Done()
			}
		})

	if err != nil {
		t.Errorf("Failed to subscribe: %v", err)
		return
	}

	// Wait for subscription callback
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if !subscribeSuccess {
			t.Error("Subscription was not successful")
		}
	case <-time.After(2 * time.Second):
		t.Error("Subscription timeout")
	}

	// Verify subscription properties
	if sub.Destination() != "/queue/sub-test" {
		t.Errorf("Expected destination /queue/sub-test, got %s", sub.Destination())
	}

	if sub.AckMode() != AckClient {
		t.Errorf("Expected ack mode AckClient, got %s", sub.AckMode())
	}

	if !sub.IsActive() {
		t.Error("Expected subscription to be active")
	}

	// Test unsubscription
	wg.Add(1)
	err = conn.Unsubscribe(sub, func(conn *CallbackConn, subscription *CallbackSubscription, event SubscriptionEvent, err error) {
		if err != nil {
			t.Errorf("Unsubscribe error: %v", err)
		} else {
			t.Logf("Unsubscribed from %s", subscription.Destination())
			unsubscribeSuccess = true
		}
		wg.Done()
	})

	if err != nil {
		t.Errorf("Failed to unsubscribe: %v", err)
		return
	}

	// Wait for unsubscribe completion
	unsubDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(unsubDone)
	}()

	select {
	case <-unsubDone:
		if !unsubscribeSuccess {
			t.Error("Unsubscription was not successful")
		}
	case <-time.After(2 * time.Second):
		t.Error("Unsubscription timeout")
	}

	// Verify subscription is no longer active
	if sub.IsActive() {
		t.Error("Expected subscription to be inactive after unsubscribe")
	}
}

func testStatistics(t *testing.T, conn *CallbackConn) {
	t.Log("Testing connection statistics...")

	stats := conn.GetConnectionStats()
	health := conn.GetHealthStatus()

	t.Logf("Health: %s", health)
	t.Logf("Frames sent: %d", stats.FramesSent)
	t.Logf("Frames received: %d", stats.FramesReceived)
	t.Logf("Heart-beats sent: %d", stats.HeartBeatsSent)
	t.Logf("Heart-beats received: %d", stats.HeartBeatsReceived)
	t.Logf("Connected at: %s", stats.ConnectedAt)

	// Verify that some frames were sent during the test
	if stats.FramesSent == 0 {
		t.Error("Expected some frames to be sent")
	}

	// Verify connection time is set
	if stats.ConnectedAt.IsZero() {
		t.Error("Expected ConnectedAt to be set")
	}
}

// handleServerResponses simulates a STOMP server for testing
func handleServerResponses(serverConn net.Conn) {
	reader := frame.NewReader(serverConn)
	writer := frame.NewWriter(serverConn)

	for {
		f, err := reader.Read()
		if err != nil {
			fmt.Printf("Server handler: Connection closed or error: %v\n", err)
			return // Connection closed
		}

		fmt.Printf("Server handler: Received frame: %s\n", f.Command)
		if receiptId := f.Header.Get(frame.Receipt); receiptId != "" {
			fmt.Printf("Server handler: Frame has receipt ID: %s\n", receiptId)
		}

		switch f.Command {
		case frame.CONNECT:
			// Respond with CONNECTED
			connectedFrame := frame.New(frame.CONNECTED,
				frame.Version, "1.2",
				frame.Server, "test-server",
				frame.Session, "test-session")
			fmt.Printf("Server handler: Sending CONNECTED frame\n")
			writer.Write(connectedFrame)

		case frame.DISCONNECT:
			// Respond with RECEIPT if requested
			if receiptId := f.Header.Get(frame.Receipt); receiptId != "" {
				receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
				fmt.Printf("Server handler: Sending RECEIPT for DISCONNECT (ID: %s)\n", receiptId)
				writer.Write(receiptFrame)
			}
			return

		case frame.SEND:
			// Respond with RECEIPT if requested
			if receiptId := f.Header.Get(frame.Receipt); receiptId != "" {
				receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
				fmt.Printf("Server handler: Sending RECEIPT for SEND (ID: %s)\n", receiptId)
				writer.Write(receiptFrame)
			}

		case frame.BEGIN:
			// Transaction begun, send receipt if requested
			if receiptId := f.Header.Get(frame.Receipt); receiptId != "" {
				receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
				fmt.Printf("Server handler: Sending RECEIPT for BEGIN (ID: %s)\n", receiptId)
				writer.Write(receiptFrame)
			} else {
				fmt.Printf("Server handler: BEGIN transaction without receipt\n")
			}

		case frame.COMMIT:
			// Transaction committed, send receipt if requested
			if receiptId := f.Header.Get(frame.Receipt); receiptId != "" {
				receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
				fmt.Printf("Server handler: Sending RECEIPT for COMMIT (ID: %s)\n", receiptId)
				writer.Write(receiptFrame)
			} else {
				fmt.Printf("Server handler: COMMIT transaction without receipt\n")
			}

		case frame.ABORT:
			// Transaction aborted, send receipt if requested
			if receiptId := f.Header.Get(frame.Receipt); receiptId != "" {
				receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
				fmt.Printf("Server handler: Sending RECEIPT for ABORT (ID: %s)\n", receiptId)
				writer.Write(receiptFrame)
			} else {
				fmt.Printf("Server handler: ABORT transaction without receipt\n")
			}

		case frame.SUBSCRIBE:
			// Respond with RECEIPT if requested
			if receiptId := f.Header.Get(frame.Receipt); receiptId != "" {
				receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
				fmt.Printf("Server handler: Sending RECEIPT for SUBSCRIBE (ID: %s)\n", receiptId)
				writer.Write(receiptFrame)
			}

		case frame.UNSUBSCRIBE:
			// Respond with RECEIPT if requested
			if receiptId := f.Header.Get(frame.Receipt); receiptId != "" {
				receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
				fmt.Printf("Server handler: Sending RECEIPT for UNSUBSCRIBE (ID: %s)\n", receiptId)
				writer.Write(receiptFrame)
			}

		default:
			fmt.Printf("Server handler: Unknown frame command: %s\n", f.Command)
		}
	}
}

func TestCallbackConn_ErrorRecovery(t *testing.T) {
	// Test error recovery scenarios
	serverConn, clientConn := newTestConnections()
	defer serverConn.Close()
	defer clientConn.Close()

	callbackConn := NewCallbackConn(cbio.WrapReadWriteCloser(clientConn))

	// Set up error recovery callback
	callbackConn.SetErrorRecoveryCallback(func(conn *CallbackConn, err error, recoveryAction RecoveryAction) RecoveryDecision {
		t.Logf("Error recovery: %v, suggested action: %s", err, recoveryAction)
		return DecisionProceed
	})

	// Set up error callback
	callbackConn.SetErrorCallback(func(conn *CallbackConn, err error) {
		t.Logf("Error occurred: %v", err)
	})

	// For this test, we'll verify the callbacks are properly set
	// In a real scenario, we'd trigger actual errors and test recovery
	if callbackConn.errorRecoveryCallback == nil {
		t.Error("Error recovery callback was not set")
	}

	if callbackConn.errorCallback == nil {
		t.Error("Error callback was not set")
	}
}

func TestCallbackConn_HeartBeatNegotiation(t *testing.T) {
	// Test heart-beat negotiation
	serverConn, clientConn := newTestConnections()
	defer serverConn.Close()
	defer clientConn.Close()

	callbackConn := NewCallbackConn(cbio.WrapReadWriteCloser(clientConn))

	var heartBeatNegotiated bool
	var wg sync.WaitGroup

	// Set up heart-beat callback
	callbackConn.SetHeartBeatCallback(func(conn *CallbackConn, event HeartBeatEvent, err error) {
		if event == HeartBeatNegotiated && err == nil {
			heartBeatNegotiated = true
			t.Log("Heart-beat negotiated successfully")
		}
	})

	// Start server that responds with heart-beat
	go func() {
		reader := frame.NewReader(serverConn)
		writer := frame.NewWriter(serverConn)

		f, err := reader.Read()
		if err != nil {
			return
		}

		if f.Command == frame.CONNECT {
			// Respond with CONNECTED including heart-beat
			connectedFrame := frame.New(frame.CONNECTED,
				frame.Version, "1.2",
				frame.Server, "test-server",
				frame.Session, "test-session",
				frame.HeartBeat, "10000,10000")
			writer.Write(connectedFrame)
		}
	}()

	// Connect
	wg.Add(1)
	err := callbackConn.Connect(func(conn *CallbackConn, session, server string, version Version) {
		t.Log("Connected with heart-beat negotiation")
		wg.Done()
	})

	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Wait for connection
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if !heartBeatNegotiated {
			t.Error("Heart-beat was not negotiated")
		}
	case <-time.After(3 * time.Second):
		t.Error("Connection timeout")
	}
}
