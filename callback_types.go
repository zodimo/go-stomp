package stomp

import (
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
