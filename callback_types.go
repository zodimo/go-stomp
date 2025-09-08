package stomp

import (
	"time"

	"github.com/go-stomp/stomp/v3/frame"
)

// ConnectionState represents the current state of a callback connection
type ConnectionState int

const (
	// Disconnected indicates the connection is not established
	Disconnected ConnectionState = iota
	// Connecting indicates the connection is being established
	Connecting
	// Connected indicates the connection is established and ready
	Connected
	// Disconnecting indicates the connection is being closed
	Disconnecting
)

// String returns a string representation of the connection state
func (cs ConnectionState) String() string {
	switch cs {
	case Disconnected:
		return "Disconnected"
	case Connecting:
		return "Connecting"
	case Connected:
		return "Connected"
	case Disconnecting:
		return "Disconnecting"
	default:
		return "Unknown"
	}
}

// ConnectCallback is called when a connection is successfully established
type ConnectCallback func(conn *CallbackConn, session string, server string, version Version)

// DisconnectCallback is called when a connection is closed
type DisconnectCallback func(conn *CallbackConn, err error)

// StateChangeCallback is called when the connection state changes
type StateChangeCallback func(conn *CallbackConn, oldState, newState ConnectionState)

// ErrorCallback is called when an error occurs
type ErrorCallback func(conn *CallbackConn, err error)

// SendCallback is called when a send operation completes
type SendCallback func(conn *CallbackConn, destination string, err error)

// MessageHandler is called when a message is received for a subscription
type MessageHandler func(conn *CallbackConn, message *CallbackMessage)

// SubscriptionCallback is called for subscription lifecycle events
type SubscriptionCallback func(conn *CallbackConn, subscription *CallbackSubscription, event SubscriptionEvent, err error)

// AckCallback is called when an acknowledgment operation completes
type AckCallback func(conn *CallbackConn, messageId string, err error)

// SubscriptionEvent represents the type of subscription event
type SubscriptionEvent int

const (
	// SubscriptionCreated indicates a subscription was successfully created
	SubscriptionCreated SubscriptionEvent = iota
	// SubscriptionActive indicates a subscription is active and receiving messages
	SubscriptionActive
	// SubscriptionUnsubscribed indicates a subscription was unsubscribed
	SubscriptionUnsubscribed
	// SubscriptionError indicates an error occurred with the subscription
	SubscriptionError
)

// String returns a string representation of the subscription event
func (se SubscriptionEvent) String() string {
	switch se {
	case SubscriptionCreated:
		return "Created"
	case SubscriptionActive:
		return "Active"
	case SubscriptionUnsubscribed:
		return "Unsubscribed"
	case SubscriptionError:
		return "Error"
	default:
		return "Unknown"
	}
}

// CallbackSubscription represents a subscription in the callback-style client
type CallbackSubscription struct {
	id          string
	destination string
	ackMode     AckMode
	handler     MessageHandler
	conn        *CallbackConn
	active      bool
}

// Id returns the subscription ID
func (s *CallbackSubscription) Id() string {
	return s.id
}

// Destination returns the subscription destination
func (s *CallbackSubscription) Destination() string {
	return s.destination
}

// AckMode returns the acknowledgment mode for the subscription
func (s *CallbackSubscription) AckMode() AckMode {
	return s.ackMode
}

// IsActive returns whether the subscription is currently active
func (s *CallbackSubscription) IsActive() bool {
	return s.active
}

// CallbackMessage represents a message received via callback-style client
type CallbackMessage struct {
	// Header contains the message headers
	Header *frame.Header
	// Body contains the message body
	Body []byte
	// Subscription is the subscription that received this message
	Subscription *CallbackSubscription
	// Conn is the connection that received this message
	Conn *CallbackConn
	// ackId is used for acknowledgment operations
	ackId string
	// Destination is the message destination
	Destination string
	// ContentType is the MIME content type
	ContentType string
}

// ShouldAck returns true if this message should be acknowledged
func (msg *CallbackMessage) ShouldAck() bool {
	if msg.Subscription == nil {
		return false
	}
	return msg.Subscription.AckMode() != AckAuto
}

// HeartBeatEvent represents the type of heart-beat event
type HeartBeatEvent int

const (
	// HeartBeatNegotiated indicates heart-beat parameters were negotiated
	HeartBeatNegotiated HeartBeatEvent = iota
	// HeartBeatSent indicates a heart-beat frame was sent
	HeartBeatSent
	// HeartBeatReceived indicates a heart-beat frame was received
	HeartBeatReceived
	// HeartBeatTimeout indicates a heart-beat timeout occurred
	HeartBeatTimeout
)

// String returns a string representation of the heart-beat event
func (hbe HeartBeatEvent) String() string {
	switch hbe {
	case HeartBeatNegotiated:
		return "Negotiated"
	case HeartBeatSent:
		return "Sent"
	case HeartBeatReceived:
		return "Received"
	case HeartBeatTimeout:
		return "Timeout"
	default:
		return "Unknown"
	}
}

// TransactionEvent represents the type of transaction event
type TransactionEvent int

const (
	// TransactionBegan indicates a transaction was started
	TransactionBegan TransactionEvent = iota
	// TransactionCommitted indicates a transaction was committed
	TransactionCommitted
	// TransactionAborted indicates a transaction was aborted
	TransactionAborted
	// TransactionError indicates an error occurred with the transaction
	TransactionError
)

// String returns a string representation of the transaction event
func (te TransactionEvent) String() string {
	switch te {
	case TransactionBegan:
		return "Began"
	case TransactionCommitted:
		return "Committed"
	case TransactionAborted:
		return "Aborted"
	case TransactionError:
		return "Error"
	default:
		return "Unknown"
	}
}

// ConnectionHealth represents the health status of a connection
type ConnectionHealth int

const (
	// HealthUnknown indicates the health status is unknown
	HealthUnknown ConnectionHealth = iota
	// HealthConnecting indicates the connection is being established
	HealthConnecting
	// HealthHealthy indicates the connection is healthy and operational
	HealthHealthy
	// HealthDegraded indicates the connection has issues but is still operational
	HealthDegraded
	// HealthUnhealthy indicates the connection has serious issues
	HealthUnhealthy
	// HealthDisconnected indicates the connection is disconnected
	HealthDisconnected
)

// String returns a string representation of the connection health
func (ch ConnectionHealth) String() string {
	switch ch {
	case HealthUnknown:
		return "Unknown"
	case HealthConnecting:
		return "Connecting"
	case HealthHealthy:
		return "Healthy"
	case HealthDegraded:
		return "Degraded"
	case HealthUnhealthy:
		return "Unhealthy"
	case HealthDisconnected:
		return "Disconnected"
	default:
		return "Unknown"
	}
}

// RecoveryAction represents the type of recovery action
type RecoveryAction int

const (
	// RecoveryReconnect indicates a reconnection attempt should be made
	RecoveryReconnect RecoveryAction = iota
	// RecoveryRetry indicates the operation should be retried
	RecoveryRetry
	// RecoverySkip indicates the operation should be skipped
	RecoverySkip
	// RecoveryFail indicates the operation should fail
	RecoveryFail
)

// String returns a string representation of the recovery action
func (ra RecoveryAction) String() string {
	switch ra {
	case RecoveryReconnect:
		return "Reconnect"
	case RecoveryRetry:
		return "Retry"
	case RecoverySkip:
		return "Skip"
	case RecoveryFail:
		return "Fail"
	default:
		return "Unknown"
	}
}

// RecoveryDecision represents the decision made by error recovery callback
type RecoveryDecision int

const (
	// DecisionProceed indicates to proceed with the suggested recovery action
	DecisionProceed RecoveryDecision = iota
	// DecisionRetry indicates to retry the recovery action
	DecisionRetry
	// DecisionAbort indicates to abort the recovery action
	DecisionAbort
)

// String returns a string representation of the recovery decision
func (rd RecoveryDecision) String() string {
	switch rd {
	case DecisionProceed:
		return "Proceed"
	case DecisionRetry:
		return "Retry"
	case DecisionAbort:
		return "Abort"
	default:
		return "Unknown"
	}
}

// TransactionState represents the state of a transaction
type TransactionState int

const (
	// TxStateActive indicates the transaction is active
	TxStateActive TransactionState = iota
	// TxStateCommitted indicates the transaction has been committed
	TxStateCommitted
	// TxStateAborted indicates the transaction has been aborted
	TxStateAborted
)

// String returns a string representation of the transaction state
func (ts TransactionState) String() string {
	switch ts {
	case TxStateActive:
		return "Active"
	case TxStateCommitted:
		return "Committed"
	case TxStateAborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

// HeartBeatCallback is called for heart-beat events
type HeartBeatCallback func(conn *CallbackConn, event HeartBeatEvent, err error)

// TransactionCallback is called for transaction events
type TransactionCallback func(conn *CallbackConn, tx *CallbackTransaction, event TransactionEvent, err error)

// ErrorRecoveryCallback is called when an error occurs and recovery options are available
type ErrorRecoveryCallback func(conn *CallbackConn, err error, recoveryAction RecoveryAction) RecoveryDecision

// HealthStatusCallback is called when the connection health status changes
type HealthStatusCallback func(conn *CallbackConn, oldStatus, newStatus ConnectionHealth)

// ShutdownCallback is called when the connection is being shut down
type ShutdownCallback func(conn *CallbackConn, err error)

// CallbackConnectionStats contains connection statistics for callback connections
type CallbackConnectionStats struct {
	// FramesSent is the number of frames sent
	FramesSent int64
	// FramesReceived is the number of frames received
	FramesReceived int64
	// HeartBeatsSent is the number of heart-beat frames sent
	HeartBeatsSent int64
	// HeartBeatsReceived is the number of heart-beat frames received
	HeartBeatsReceived int64
	// LastHeartBeatSent is the timestamp of the last heart-beat sent
	LastHeartBeatSent time.Time
	// LastHeartBeatReceived is the timestamp of the last heart-beat received
	LastHeartBeatReceived time.Time
	// ConnectedAt is the timestamp when the connection was established
	ConnectedAt time.Time
	// LastError is the last error that occurred
	LastError error
}

// CallbackTransaction represents a transaction in the callback-style client
type CallbackTransaction struct {
	id       string
	conn     *CallbackConn
	state    TransactionState
	callback TransactionCallback
}

// Id returns the transaction ID
func (tx *CallbackTransaction) Id() string {
	return tx.id
}

// State returns the current transaction state
func (tx *CallbackTransaction) State() TransactionState {
	return tx.state
}

// Conn returns the connection associated with this transaction
func (tx *CallbackTransaction) Conn() *CallbackConn {
	return tx.conn
}
