package stomp

import (
	"time"

	"github.com/go-stomp/stomp/v3/frame"
)

// Commit commits the transaction with callback notification
func (tx *CallbackTransaction) Commit(callback TransactionCallback) error {
	if tx.state != TxStateActive {
		return ErrCompletedTransaction
	}

	// Create COMMIT frame
	receiptId := allocateId()
	commitFrame := frame.New(frame.COMMIT, frame.Transaction, tx.id, frame.Receipt, receiptId)

	// Register operation BEFORE sending frame to prevent race condition
	responseCh, errorCh, cancel, err := tx.conn.registerAndWaitForReceipt(
		"commit",
		receiptId,
		tx.conn.frameRouter.operationTimeouts.Transaction) // Use transaction timeout
	if err != nil {
		if callback != nil {
			go callback(tx.conn, tx, TransactionError, err)
		}
		return err
	}
	defer cancel()

	// Send COMMIT frame AFTER registering operation
	writer := frame.NewUnwrapCbioWriter(tx.conn.conn)
	err = writer.WriteSync(commitFrame)
	if err != nil {
		// Unregister pending operation on send failure
		tx.conn.frameRouter.UnregisterPendingOperation(receiptId)
		if callback != nil {
			go callback(tx.conn, tx, TransactionError, err)
		}
		return err
	}

	// Update statistics
	tx.conn.mu.Lock()
	tx.conn.stats.FramesSent++
	tx.conn.mu.Unlock()

	// Wait for receipt or error
	select {
	case <-responseCh:
		// Receipt received, commit successful
		// Only update transaction state and remove from map AFTER receipt confirmation
		tx.state = TxStateCommitted
		tx.conn.mu.Lock()
		delete(tx.conn.transactions, tx.id)
		tx.conn.mu.Unlock()

		if callback != nil {
			go callback(tx.conn, tx, TransactionCommitted, nil)
		}
	case err := <-errorCh:
		// Error occurred
		if callback != nil {
			go callback(tx.conn, tx, TransactionError, err)
		}
		return err
	case <-time.After(tx.conn.frameRouter.operationTimeouts.Transaction): // Use transaction timeout
		// Timeout occurred
		tx.conn.frameRouter.UnregisterPendingOperation(receiptId)
		err := ErrTransactionTimeout // Use specific transaction timeout error
		if callback != nil {
			go callback(tx.conn, tx, TransactionError, err)
		}
		return err
	}

	return nil
}

// Abort aborts the transaction with callback notification
func (tx *CallbackTransaction) Abort(callback TransactionCallback) error {
	if tx.state != TxStateActive {
		return ErrCompletedTransaction
	}

	// Create ABORT frame
	receiptId := allocateId()
	abortFrame := frame.New(frame.ABORT, frame.Transaction, tx.id, frame.Receipt, receiptId)

	// Register operation BEFORE sending frame to prevent race condition
	responseCh, errorCh, cancel, err := tx.conn.registerAndWaitForReceipt(
		"abort",
		receiptId,
		tx.conn.frameRouter.operationTimeouts.Transaction) // Use transaction timeout
	if err != nil {
		if callback != nil {
			go callback(tx.conn, tx, TransactionError, err)
		}
		return err
	}
	defer cancel()

	// Send ABORT frame AFTER registering operation
	writer := frame.NewUnwrapCbioWriter(tx.conn.conn)
	err = writer.WriteSync(abortFrame)
	if err != nil {
		// Unregister pending operation on send failure
		tx.conn.frameRouter.UnregisterPendingOperation(receiptId)
		if callback != nil {
			go callback(tx.conn, tx, TransactionError, err)
		}
		return err
	}

	// Update statistics
	tx.conn.mu.Lock()
	tx.conn.stats.FramesSent++
	tx.conn.mu.Unlock()

	// Wait for receipt or error
	select {
	case <-responseCh:
		// Receipt received, abort successful
		// Only update transaction state and remove from map AFTER receipt confirmation
		tx.state = TxStateAborted
		tx.conn.mu.Lock()
		delete(tx.conn.transactions, tx.id)
		tx.conn.mu.Unlock()

		if callback != nil {
			go callback(tx.conn, tx, TransactionAborted, nil)
		}
	case err := <-errorCh:
		// Error occurred
		if callback != nil {
			go callback(tx.conn, tx, TransactionError, err)
		}
		return err
	case <-time.After(tx.conn.frameRouter.operationTimeouts.Transaction): // Use transaction timeout
		// Timeout occurred
		tx.conn.frameRouter.UnregisterPendingOperation(receiptId)
		err := ErrTransactionTimeout // Use specific transaction timeout error
		if callback != nil {
			go callback(tx.conn, tx, TransactionError, err)
		}
		return err
	}

	return nil
}

// Send sends a message within the transaction
func (tx *CallbackTransaction) Send(destination, contentType string, body []byte, callback SendCallback, opts ...func(*frame.Frame) error) error {
	if tx.state != TxStateActive {
		return ErrCompletedTransaction
	}

	if tx.conn.GetState() != Connected {
		return ErrNotConnected
	}

	// Create send frame using existing createSendFrame logic
	f, err := createSendFrame(destination, contentType, body, opts)
	if err != nil {
		if callback != nil {
			go callback(tx.conn, destination, err)
		}
		return err
	}

	// Add transaction header
	f.Header.Set(frame.Transaction, tx.id)

	// Send frame
	writer := frame.NewUnwrapCbioWriter(tx.conn.conn)
	err = writer.WriteSync(f)
	if err != nil {
		if callback != nil {
			go callback(tx.conn, destination, err)
		}
		return err
	}

	// Update statistics
	tx.conn.mu.Lock()
	tx.conn.stats.FramesSent++
	tx.conn.mu.Unlock()

	// Notify success
	if callback != nil {
		go callback(tx.conn, destination, nil)
	}

	return nil
}
