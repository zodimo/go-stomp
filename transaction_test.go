package stomp

import (
	"sync"
	"testing"
	"time"

	"github.com/zodimo/go-netkit/cbio"
)

func TestTransactionOperations(t *testing.T) {
	// Create test server connection
	serverConn, clientConn := newTestConnections()
	defer serverConn.Close()
	defer clientConn.Close()

	// Create callback connection
	callbackConn := NewCallbackConn(cbio.WrapReadWriteCloser(clientConn))

	// Start server response handler
	go handleServerResponses(serverConn)

	// Connect
	var wg sync.WaitGroup
	wg.Add(1)
	err := callbackConn.Connect(func(conn *CallbackConn, session, server string, version Version) {
		t.Logf("Connected: session=%s, server=%s, version=%s", session, server, version)
		wg.Done()
	})

	if err != nil {
		t.Fatalf("Failed to initiate connection: %v", err)
	}

	// Wait for connection with timeout
	connectDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(connectDone)
	}()

	select {
	case <-connectDone:
		t.Log("Connection successful")
	case <-time.After(5 * time.Second):
		t.Fatal("Connection timeout")
	}

	// Test with longer timeout to ensure we catch actual issues, not just timing
	testTimeout := 5 * time.Second

	// Test Begin Transaction
	t.Run("BeginTransaction", func(t *testing.T) {
		var txBeginSuccess bool
		var wg sync.WaitGroup

		wg.Add(1)
		tx, err := callbackConn.Begin(func(conn *CallbackConn, tx *CallbackTransaction, event TransactionEvent, err error) {
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
		tx, err := callbackConn.Begin(func(conn *CallbackConn, tx *CallbackTransaction, event TransactionEvent, err error) {
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
		tx, err := callbackConn.Begin(func(conn *CallbackConn, tx *CallbackTransaction, event TransactionEvent, err error) {
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

	// Disconnect
	disconnectCh := make(chan struct{})
	err = callbackConn.Disconnect(func(conn *CallbackConn, err error) {
		t.Logf("Disconnected: %v", err)
		close(disconnectCh)
	})

	if err != nil {
		t.Fatalf("Failed to initiate disconnection: %v", err)
	}

	// Give disconnection a moment to complete
	time.Sleep(500 * time.Millisecond)
	t.Log("Disconnection completed")
}
