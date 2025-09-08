package stomp

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
	"github.com/zodimo/go-netkit/cbio"
)

// CallbackConn represents a callback-style STOMP connection using cbio interfaces
type CallbackConn struct {
	// cbio interface for asynchronous I/O
	conn cbio.ReadWriteCloser
	// standard io adapter for frame operations
	ioAdapter io.ReadWriteCloser

	// Connection state management
	mu    sync.RWMutex
	state ConnectionState

	// Protocol information
	version Version
	session string
	server  string

	// Timeouts (using patterns from existing Conn)
	readTimeout               time.Duration
	writeTimeout              time.Duration
	msgSendTimeout            time.Duration
	rcvReceiptTimeout         time.Duration
	disconnectReceiptTimeout  time.Duration
	unsubscribeReceiptTimeout time.Duration
	hbGracePeriodMultiplier   float64

	// Callback handlers
	connectCallback     ConnectCallback
	disconnectCallback  DisconnectCallback
	stateChangeCallback StateChangeCallback
	errorCallback       ErrorCallback

	// Message handling
	subscriptions        map[string]*CallbackSubscription
	messageHandlers      map[string]MessageHandler
	sendCallback         SendCallback
	subscriptionCallback SubscriptionCallback
	ackCallback          AckCallback

	// Connection options
	options connOptions
}

// CallbackConnOption represents options for callback connections
type CallbackConnOption func(*CallbackConn) error

// NewCallbackConn creates a new callback-style STOMP connection
func NewCallbackConn(conn io.ReadWriteCloser, opts ...CallbackConnOption) *CallbackConn {
	cbioConn := cbio.WrapReadWriteCloser(conn)

	c := &CallbackConn{
		conn:      cbioConn,
		ioAdapter: conn,
		state:     Disconnected,
		// Set default timeouts matching existing Conn
		msgSendTimeout:            DefaultMsgSendTimeout,
		rcvReceiptTimeout:         DefaultRcvReceiptTimeout,
		disconnectReceiptTimeout:  DefaultDisconnectReceiptTimeout,
		unsubscribeReceiptTimeout: DefaultUnsubscribeReceiptTimeout,
		hbGracePeriodMultiplier:   1.0,
		// Initialize message handling maps
		subscriptions:   make(map[string]*CallbackSubscription),
		messageHandlers: make(map[string]MessageHandler),
	}

	// Initialize default options
	c.options = connOptions{
		ReadTimeout:                    time.Minute,
		WriteTimeout:                   time.Minute,
		HeartBeatGracePeriodMultiplier: 1.0,
		AcceptVersions:                 []string{string(V10), string(V11), string(V12)},
		FrameCommand:                   "CONNECT",
	}

	// Apply connection options
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}

	// Apply options to connection
	c.readTimeout = c.options.ReadTimeout
	c.writeTimeout = c.options.WriteTimeout

	return c
}

// GetState returns the current connection state
func (c *CallbackConn) GetState() ConnectionState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// SetStateChangeCallback sets the callback for state changes
func (c *CallbackConn) SetStateChangeCallback(callback StateChangeCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stateChangeCallback = callback
}

// SetErrorCallback sets the callback for errors
func (c *CallbackConn) SetErrorCallback(callback ErrorCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errorCallback = callback
}

// SetSendCallback sets the callback for send operations
func (c *CallbackConn) SetSendCallback(callback SendCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sendCallback = callback
}

// SetSubscriptionCallback sets the callback for subscription events
func (c *CallbackConn) SetSubscriptionCallback(callback SubscriptionCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subscriptionCallback = callback
}

// SetAckCallback sets the callback for acknowledgment operations
func (c *CallbackConn) SetAckCallback(callback AckCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ackCallback = callback
}

// setState changes the connection state and notifies callbacks
func (c *CallbackConn) setState(newState ConnectionState) {
	c.mu.Lock()
	oldState := c.state
	c.state = newState
	stateCallback := c.stateChangeCallback
	c.mu.Unlock()

	// Call state change callback if set
	if stateCallback != nil && oldState != newState {
		stateCallback(c, oldState, newState)
	}
}

// notifyError calls the error callback if set
func (c *CallbackConn) notifyError(err error) {
	c.mu.RLock()
	errorCallback := c.errorCallback
	c.mu.RUnlock()

	if errorCallback != nil {
		errorCallback(c, err)
	}
}

// Connect initiates the STOMP connection with callback-based handling
func (c *CallbackConn) Connect(connectCallback ConnectCallback) error {
	if c.GetState() != Disconnected {
		return ErrAlreadyConnected
	}

	c.setState(Connecting)
	c.mu.Lock()
	c.connectCallback = connectCallback
	c.mu.Unlock()

	// Create CONNECT frame using options
	connectFrame, err := c.createConnectFrame()
	if err != nil {
		c.setState(Disconnected)
		c.notifyError(err)
		return err
	}

	// Start the connection process asynchronously
	go c.performConnect(connectFrame)

	return nil
}

// createConnectFrame creates a CONNECT frame from the connection options
func (c *CallbackConn) createConnectFrame() (*frame.Frame, error) {
	f := frame.New(c.options.FrameCommand)

	if c.options.Host != "" {
		f.Header.Set(frame.Host, c.options.Host)
	}

	// heart-beat
	{
		send := c.options.WriteTimeout / time.Millisecond
		recv := c.options.ReadTimeout / time.Millisecond
		f.Header.Set(frame.HeartBeat, fmt.Sprintf("%d,%d", send, recv))
	}

	// login, passcode
	if c.options.Login != "" || c.options.Passcode != "" {
		f.Header.Set(frame.Login, c.options.Login)
		f.Header.Set(frame.Passcode, c.options.Passcode)
	}

	// accept-version
	f.Header.Set(frame.AcceptVersion, strings.Join(c.options.AcceptVersions, ","))

	// custom header entries
	f.Header.AddHeader(c.options.Header)

	return f, nil
}

// performConnect handles the actual connection process
func (c *CallbackConn) performConnect(connectFrame *frame.Frame) {
	// Create frame writer and reader using the standard io adapter
	writer := frame.NewWriter(c.ioAdapter)
	reader := frame.NewReader(c.ioAdapter)

	// Send CONNECT frame
	err := writer.Write(connectFrame)
	if err != nil {
		c.setState(Disconnected)
		c.notifyError(err)
		return
	}

	// Wait for CONNECTED response with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	responseCh := make(chan *frame.Frame, 1)
	errorCh := make(chan error, 1)

	// Read response asynchronously
	go func() {
		response, err := reader.Read()
		if err != nil {
			errorCh <- err
			return
		}
		responseCh <- response
	}()

	select {
	case response := <-responseCh:
		c.handleConnectResponse(response)
	case err := <-errorCh:
		c.setState(Disconnected)
		c.notifyError(err)
	case <-ctx.Done():
		c.setState(Disconnected)
		c.notifyError(ctx.Err())
	}
}

// handleConnectResponse processes the CONNECTED frame response
func (c *CallbackConn) handleConnectResponse(response *frame.Frame) {
	if response.Command != frame.CONNECTED {
		err := newError(response)
		c.setState(Disconnected)
		c.notifyError(err)
		return
	}

	// Extract connection details from CONNECTED frame
	c.server = response.Header.Get(frame.Server)
	c.session = response.Header.Get(frame.Session)

	// Handle version negotiation
	if versionString := response.Header.Get(frame.Version); versionString != "" {
		version := Version(versionString)
		if err := version.CheckSupported(); err != nil {
			c.setState(Disconnected)
			c.notifyError(Error{
				Message: err.Error(),
				Frame:   response,
			})
			return
		}
		c.version = version
	} else {
		// no version in the response, so assume version 1.0
		c.version = V10
	}

	// Handle heart-beat negotiation
	if heartBeat, ok := response.Header.Contains(frame.HeartBeat); ok {
		readTimeout, writeTimeout, err := frame.ParseHeartBeat(heartBeat)
		if err != nil {
			c.setState(Disconnected)
			c.notifyError(Error{
				Message: err.Error(),
				Frame:   response,
			})
			return
		}

		if readTimeout < c.options.ReadTimeout {
			readTimeout = c.options.ReadTimeout
		}

		c.readTimeout = readTimeout
		c.writeTimeout = writeTimeout

		if c.readTimeout > 0 {
			// Add time to the read timeout to account for time delay
			c.readTimeout += DefaultHeartBeatError
		}
		if c.writeTimeout > DefaultHeartBeatError {
			// Reduce time from the write timeout
			c.writeTimeout -= DefaultHeartBeatError
		}
	}

	// Connection successful
	c.setState(Connected)

	// Start message processing loop
	go c.startMessageProcessing()

	// Call the connect callback
	c.mu.RLock()
	callback := c.connectCallback
	c.mu.RUnlock()

	if callback != nil {
		callback(c, c.session, c.server, c.version)
	}
}

// Disconnect initiates graceful disconnection with callback-based handling
func (c *CallbackConn) Disconnect(disconnectCallback DisconnectCallback) error {
	currentState := c.GetState()
	if currentState == Disconnected {
		return ErrAlreadyClosed
	}
	if currentState == Disconnecting {
		return nil // Already disconnecting
	}

	c.setState(Disconnecting)
	c.mu.Lock()
	c.disconnectCallback = disconnectCallback
	c.mu.Unlock()

	// Start the disconnect process asynchronously
	go c.performDisconnect()

	return nil
}

// performDisconnect handles the actual disconnection process
func (c *CallbackConn) performDisconnect() {
	// Create frame writer and reader using the standard io adapter
	writer := frame.NewWriter(c.ioAdapter)
	reader := frame.NewReader(c.ioAdapter)

	// Create DISCONNECT frame with receipt
	receiptId := allocateId()
	disconnectFrame := frame.New(frame.DISCONNECT, frame.Receipt, receiptId)

	// Send DISCONNECT frame
	err := writer.Write(disconnectFrame)
	if err != nil {
		c.finalizeDisconnect(err)
		return
	}

	// Wait for RECEIPT response with timeout
	ctx, cancel := context.WithTimeout(context.Background(), c.disconnectReceiptTimeout)
	defer cancel()

	responseCh := make(chan *frame.Frame, 1)
	errorCh := make(chan error, 1)

	// Read response asynchronously
	go func() {
		for {
			response, err := reader.Read()
			if err != nil {
				errorCh <- err
				return
			}

			// Check if this is the receipt we're waiting for
			if response.Command == frame.RECEIPT {
				if response.Header.Get(frame.ReceiptId) == receiptId {
					responseCh <- response
					return
				}
				// Continue reading if it's not our receipt
			} else if response.Command == frame.ERROR {
				errorCh <- newError(response)
				return
			}
		}
	}()

	select {
	case <-responseCh:
		// Receipt received, disconnect successful
		c.finalizeDisconnect(nil)
	case err := <-errorCh:
		// Error occurred
		c.finalizeDisconnect(err)
	case <-ctx.Done():
		// Timeout occurred
		c.finalizeDisconnect(ErrDisconnectReceiptTimeout)
	}
}

// finalizeDisconnect completes the disconnection process
func (c *CallbackConn) finalizeDisconnect(err error) {
	// Close the underlying connection
	if closeErr := c.ioAdapter.Close(); closeErr != nil && err == nil {
		err = closeErr
	}

	// Set state to disconnected
	c.setState(Disconnected)

	// Call the disconnect callback
	c.mu.RLock()
	callback := c.disconnectCallback
	c.mu.RUnlock()

	if callback != nil {
		callback(c, err)
	}
}

// startMessageProcessing starts the message processing loop
func (c *CallbackConn) startMessageProcessing() {
	reader := frame.NewReader(c.ioAdapter)

	for c.GetState() == Connected {
		f, err := reader.Read()
		if err != nil {
			c.notifyError(err)
			break
		}

		if f == nil {
			// heart-beat received
			continue
		}

		switch f.Command {
		case frame.MESSAGE:
			c.handleMessageFrame(f)
		case frame.RECEIPT:
			// Receipt frames are handled by individual operations
			continue
		case frame.ERROR:
			c.notifyError(newError(f))
			c.setState(Disconnected)
			return
		}
	}
}

// handleMessageFrame processes incoming MESSAGE frames
func (c *CallbackConn) handleMessageFrame(f *frame.Frame) {
	// Get subscription ID from frame
	subscriptionId, ok := f.Header.Contains(frame.Subscription)
	if !ok {
		// No subscription ID, ignore the message
		return
	}

	// Find the subscription and handler
	c.mu.RLock()
	subscription, exists := c.subscriptions[subscriptionId]
	handler, hasHandler := c.messageHandlers[subscriptionId]
	c.mu.RUnlock()

	if !exists || !hasHandler || !subscription.active {
		// No active subscription or handler, ignore the message
		return
	}

	// Create callback message
	message := &CallbackMessage{
		Header:       f.Header,
		Body:         f.Body,
		Subscription: subscription,
		Conn:         c,
		Destination:  f.Header.Get(frame.Destination),
		ContentType:  f.Header.Get(frame.ContentType),
	}

	// Set ack ID based on acknowledgment mode and protocol version
	switch subscription.ackMode {
	case AckClient, AckClientIndividual:
		if messageId, ok := f.Header.Contains(frame.MessageId); ok {
			message.ackId = messageId
		}
	}

	// Call the message handler asynchronously to avoid blocking the processing loop
	go handler(c, message)
}
