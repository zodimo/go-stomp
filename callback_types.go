package stomp

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
