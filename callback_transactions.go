package stomp

import (
	"github.com/go-stomp/stomp/v3/frame"
)

// Commit commits the transaction with callback notification
func (tx *CallbackTransaction) Commit(callback TransactionCallback) error {
	if tx.state != TxStateActive {
		return ErrCompletedTransaction
	}

	// Create COMMIT frame
	commitFrame := frame.New(frame.COMMIT, frame.Transaction, tx.id)
	writer := frame.NewWriter(tx.conn.ioAdapter)
	err := writer.Write(commitFrame)
	if err != nil {
		// Notify error
		if callback != nil {
			go callback(tx.conn, tx, TransactionError, err)
		}
		return err
	}

	// Update transaction state
	tx.state = TxStateCommitted

	// Remove transaction from connection map
	tx.conn.mu.Lock()
	delete(tx.conn.transactions, tx.id)
	tx.conn.stats.FramesSent++
	tx.conn.mu.Unlock()

	// Notify success
	if callback != nil {
		go callback(tx.conn, tx, TransactionCommitted, nil)
	}

	return nil
}

// Abort aborts the transaction with callback notification
func (tx *CallbackTransaction) Abort(callback TransactionCallback) error {
	if tx.state != TxStateActive {
		return ErrCompletedTransaction
	}

	// Create ABORT frame
	abortFrame := frame.New(frame.ABORT, frame.Transaction, tx.id)
	writer := frame.NewWriter(tx.conn.ioAdapter)
	err := writer.Write(abortFrame)
	if err != nil {
		// Notify error
		if callback != nil {
			go callback(tx.conn, tx, TransactionError, err)
		}
		return err
	}

	// Update transaction state
	tx.state = TxStateAborted

	// Remove transaction from connection map
	tx.conn.mu.Lock()
	delete(tx.conn.transactions, tx.id)
	tx.conn.stats.FramesSent++
	tx.conn.mu.Unlock()

	// Notify success
	if callback != nil {
		go callback(tx.conn, tx, TransactionAborted, nil)
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
	writer := frame.NewWriter(tx.conn.ioAdapter)
	err = writer.Write(f)
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
