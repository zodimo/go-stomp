package stomp

import (
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
	var disconnectSuccess bool

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
	wg.Add(1)
	err = callbackConn.Disconnect(func(conn *CallbackConn, err error) {
		t.Logf("Disconnected: %v", err)
		disconnectSuccess = (err == nil)
		wg.Done()
	})

	if err != nil {
		t.Fatalf("Failed to initiate disconnection: %v", err)
	}

	// Wait for disconnection
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if !disconnectSuccess {
			t.Fatal("Disconnection was not successful")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Disconnection timeout")
	}

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

	var txBeginSuccess, txCommitSuccess bool
	var wg sync.WaitGroup

	// Test transaction begin
	wg.Add(1)
	tx, err := conn.Begin(func(conn *CallbackConn, tx *CallbackTransaction, event TransactionEvent, err error) {
		if err != nil {
			t.Errorf("Transaction callback error (%s): %v", event, err)
		} else {
			t.Logf("Transaction event: %s (ID: %s)", event, tx.Id())
			if event == TransactionBegan {
				txBeginSuccess = true
			} else if event == TransactionCommitted {
				txCommitSuccess = true
			}
		}
		wg.Done()
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
	case <-time.After(2 * time.Second):
		t.Error("Transaction begin timeout")
	}

	// Test transactional send
	wg.Add(1)
	var txSendSuccess bool
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
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if !txSendSuccess {
			t.Error("Transactional send was not successful")
		}
	case <-time.After(2 * time.Second):
		t.Error("Transactional send timeout")
	}

	// Test transaction commit
	wg.Add(1)
	err = tx.Commit(func(conn *CallbackConn, tx *CallbackTransaction, event TransactionEvent, err error) {
		if err != nil {
			t.Errorf("Transaction commit error: %v", err)
		} else {
			t.Logf("Transaction committed: %s", tx.Id())
			txCommitSuccess = true
		}
		wg.Done()
	})

	if err != nil {
		t.Errorf("Failed to commit transaction: %v", err)
		return
	}

	// Wait for commit completion
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if !txCommitSuccess {
			t.Error("Transaction commit was not successful")
		}
	case <-time.After(2 * time.Second):
		t.Error("Transaction commit timeout")
	}

	// Verify transaction state
	if tx.State() != TxStateCommitted {
		t.Errorf("Expected transaction state TxStateCommitted, got %s", tx.State())
	}
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
				} else if event == SubscriptionUnsubscribed {
					unsubscribeSuccess = true
				}
			}
			wg.Done()
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
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
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
			return // Connection closed
		}

		switch f.Command {
		case frame.CONNECT:
			// Respond with CONNECTED
			connectedFrame := frame.New(frame.CONNECTED,
				frame.Version, "1.2",
				frame.Server, "test-server",
				frame.Session, "test-session")
			writer.Write(connectedFrame)

		case frame.DISCONNECT:
			// Respond with RECEIPT if requested
			if receiptId := f.Header.Get(frame.Receipt); receiptId != "" {
				receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
				writer.Write(receiptFrame)
			}
			return

		case frame.SEND:
			// Respond with RECEIPT if requested
			if receiptId := f.Header.Get(frame.Receipt); receiptId != "" {
				receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
				writer.Write(receiptFrame)
			}

		case frame.BEGIN:
			// Transaction begun, no response needed
			continue

		case frame.COMMIT, frame.ABORT:
			// Transaction completed, no response needed
			continue

		case frame.SUBSCRIBE:
			// Respond with RECEIPT if requested
			if receiptId := f.Header.Get(frame.Receipt); receiptId != "" {
				receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
				writer.Write(receiptFrame)
			}

		case frame.UNSUBSCRIBE:
			// Respond with RECEIPT if requested
			if receiptId := f.Header.Get(frame.Receipt); receiptId != "" {
				receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
				writer.Write(receiptFrame)
			}
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
