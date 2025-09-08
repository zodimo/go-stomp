package stomp

import (
	"context"
	"fmt"
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
	connectCallback       ConnectCallback
	disconnectCallback    DisconnectCallback
	stateChangeCallback   StateChangeCallback
	errorCallback         ErrorCallback
	heartBeatCallback     HeartBeatCallback
	healthStatusCallback  HealthStatusCallback
	errorRecoveryCallback ErrorRecoveryCallback
	shutdownCallback      ShutdownCallback

	// Message handling
	subscriptions        map[string]*CallbackSubscription
	messageHandlers      map[string]MessageHandler
	sendCallback         SendCallback
	subscriptionCallback SubscriptionCallback
	ackCallback          AckCallback

	// Health monitoring and statistics
	healthStatus ConnectionHealth
	stats        CallbackConnectionStats

	// Transaction support
	transactions        map[string]*CallbackTransaction
	transactionCallback TransactionCallback

	// Connection options
	options connOptions
}

// CallbackConnOption represents options for callback connections
type CallbackConnOption func(*CallbackConn) error

// NewCallbackConn creates a new callback-style STOMP connection
func NewCallbackConn(conn cbio.ReadWriteCloser, opts ...CallbackConnOption) *CallbackConn {

	c := &CallbackConn{
		conn:  conn,
		state: Disconnected,
		// Set default timeouts matching existing Conn
		msgSendTimeout:            DefaultMsgSendTimeout,
		rcvReceiptTimeout:         DefaultRcvReceiptTimeout,
		disconnectReceiptTimeout:  DefaultDisconnectReceiptTimeout,
		unsubscribeReceiptTimeout: DefaultUnsubscribeReceiptTimeout,
		hbGracePeriodMultiplier:   1.0,
		// Initialize message handling maps
		subscriptions:   make(map[string]*CallbackSubscription),
		messageHandlers: make(map[string]MessageHandler),
		// Initialize transaction maps
		transactions: make(map[string]*CallbackTransaction),
		// Initialize health status
		healthStatus: HealthUnknown,
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

// SetHeartBeatCallback sets the callback for heart-beat events
func (c *CallbackConn) SetHeartBeatCallback(callback HeartBeatCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.heartBeatCallback = callback
}

// SetHealthStatusCallback sets the callback for health status changes
func (c *CallbackConn) SetHealthStatusCallback(callback HealthStatusCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.healthStatusCallback = callback
}

// SetErrorRecoveryCallback sets the callback for error recovery decisions
func (c *CallbackConn) SetErrorRecoveryCallback(callback ErrorRecoveryCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errorRecoveryCallback = callback
}

// SetShutdownCallback sets the callback for shutdown notifications
func (c *CallbackConn) SetShutdownCallback(callback ShutdownCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdownCallback = callback
}

// SetTransactionCallback sets the callback for transaction events
func (c *CallbackConn) SetTransactionCallback(callback TransactionCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.transactionCallback = callback
}

// GetHealthStatus returns the current connection health status
func (c *CallbackConn) GetHealthStatus() ConnectionHealth {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.healthStatus
}

// GetConnectionStats returns a copy of the current connection statistics
func (c *CallbackConn) GetConnectionStats() CallbackConnectionStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
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

	// Update health status based on connection state
	var newHealth ConnectionHealth
	switch newState {
	case Disconnected:
		newHealth = HealthDisconnected
	case Connecting:
		newHealth = HealthConnecting
	case Connected:
		newHealth = HealthHealthy
	case Disconnecting:
		newHealth = HealthDegraded
	default:
		newHealth = HealthUnknown
	}
	c.setHealthStatus(newHealth)
}

// setHealthStatus changes the health status and notifies callbacks
func (c *CallbackConn) setHealthStatus(newStatus ConnectionHealth) {
	c.mu.Lock()
	oldStatus := c.healthStatus
	c.healthStatus = newStatus
	healthCallback := c.healthStatusCallback
	c.mu.Unlock()

	// Call health status callback if set and status changed
	if healthCallback != nil && oldStatus != newStatus {
		healthCallback(c, oldStatus, newStatus)
	}
}

// notifyError calls the error callback if set
func (c *CallbackConn) notifyError(err error) {
	c.mu.RLock()
	errorCallback := c.errorCallback
	c.mu.RUnlock()

	// Update stats
	if err != nil {
		c.mu.Lock()
		c.stats.LastError = err
		c.mu.Unlock()
	}

	if errorCallback != nil {
		errorCallback(c, err)
	}
}

// notifyHeartBeat calls the heart-beat callback if set
func (c *CallbackConn) notifyHeartBeat(event HeartBeatEvent, err error) {
	c.mu.RLock()
	heartBeatCallback := c.heartBeatCallback
	c.mu.RUnlock()

	// Update statistics based on event
	c.mu.Lock()
	switch event {
	case HeartBeatSent:
		c.stats.HeartBeatsSent++
		c.stats.LastHeartBeatSent = time.Now()
	case HeartBeatReceived:
		c.stats.HeartBeatsReceived++
		c.stats.LastHeartBeatReceived = time.Now()
	}
	c.mu.Unlock()

	if heartBeatCallback != nil {
		heartBeatCallback(c, event, err)
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
	writer := frame.NewUnwrapCbioWriter(c.conn)
	reader := frame.NewUnwrapCbioReader(c.conn)

	// Send CONNECT frame
	err := writer.WriteSync(connectFrame)
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
		response, err := reader.ReadSync()
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

		// Notify heart-beat callback
		c.notifyHeartBeat(HeartBeatNegotiated, nil)
	}

	// Connection successful
	c.setState(Connected)

	// Initialize connection statistics
	c.mu.Lock()
	c.stats.ConnectedAt = time.Now()
	c.mu.Unlock()

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
	writer := frame.NewUnwrapCbioWriter(c.conn)
	reader := frame.NewUnwrapCbioReader(c.conn)

	// Create DISCONNECT frame with receipt
	receiptId := allocateId()
	disconnectFrame := frame.New(frame.DISCONNECT, frame.Receipt, receiptId)

	// Send DISCONNECT frame
	err := writer.WriteSync(disconnectFrame)
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
			response, err := reader.ReadSync()
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
	if closeErr := c.conn.Close(); closeErr != nil && err == nil {
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

// startMessageProcessing starts the message processing loop with heart-beat monitoring
func (c *CallbackConn) startMessageProcessing() {
	reader := frame.NewUnwrapCbioReader(c.conn)
	writer := frame.NewUnwrapCbioWriter(c.conn)

	var readTimeoutChannel <-chan time.Time
	var writeTimeoutChannel <-chan time.Time
	var readTimer *time.Timer
	var writeTimer *time.Timer

	// Set up heart-beat timers if configured
	if c.readTimeout > 0 {
		readTimer = time.NewTimer(time.Duration(float64(c.readTimeout) * c.hbGracePeriodMultiplier))
		readTimeoutChannel = readTimer.C
	}
	if c.writeTimeout > 0 {
		writeTimer = time.NewTimer(c.writeTimeout)
		writeTimeoutChannel = writeTimer.C
	}

	// Create channel for incoming frames
	frameCh := make(chan *frame.Frame, 1)
	errorCh := make(chan error, 1)

	// Start frame reading goroutine
	go func() {
		for c.GetState() == Connected {
			f, err := reader.ReadSync()
			if err != nil {
				errorCh <- err
				return
			}
			frameCh <- f
		}
	}()

	// Main processing loop with heart-beat monitoring
	for c.GetState() == Connected {
		select {
		case <-readTimeoutChannel:
			// Read timeout - heart-beat not received in time
			c.notifyHeartBeat(HeartBeatTimeout, ErrClosedUnexpectedly)
			c.setHealthStatus(HealthUnhealthy)
			c.notifyError(newErrorMessage("read timeout"))
			c.setState(Disconnected)
			return

		case <-writeTimeoutChannel:
			// Write timeout - send heart-beat frame
			err := writer.WriteSync(nil)
			if err != nil {
				c.notifyError(err)
				c.setState(Disconnected)
				return
			}
			c.notifyHeartBeat(HeartBeatSent, nil)

			// Update frame statistics
			c.mu.Lock()
			c.stats.FramesSent++
			c.mu.Unlock()

			// Reset write timer
			if writeTimer != nil {
				writeTimer.Reset(c.writeTimeout)
			}

		case err := <-errorCh:
			// Error reading frame
			c.notifyError(err)
			c.setState(Disconnected)
			return

		case f := <-frameCh:
			// Reset read timer when we receive any frame
			if readTimer != nil {
				readTimer.Reset(time.Duration(float64(c.readTimeout) * c.hbGracePeriodMultiplier))
			}

			// Update frame statistics
			c.mu.Lock()
			c.stats.FramesReceived++
			c.mu.Unlock()

			if f == nil {
				// Heart-beat frame received
				c.notifyHeartBeat(HeartBeatReceived, nil)
				continue
			}

			// Process non-heart-beat frames
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

	// Clean up timers
	if readTimer != nil {
		readTimer.Stop()
	}
	if writeTimer != nil {
		writeTimer.Stop()
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

	// Set ack ID based on protocol version and acknowledgment mode
	if subscription.ackMode != AckAuto {
		switch c.version {
		case V10, V11:
			if messageId, ok := f.Header.Contains(frame.MessageId); ok {
				message.ackId = messageId
			}
		case V12:
			if ackId, ok := f.Header.Contains(frame.Ack); ok {
				message.ackId = ackId
			}
		}
	}

	// Call the message handler asynchronously to avoid blocking the processing loop
	go handler(c, message)
}

// Begin starts a new transaction and returns a CallbackTransaction
func (c *CallbackConn) Begin(callback TransactionCallback) (*CallbackTransaction, error) {
	if c.GetState() != Connected {
		return nil, ErrNotConnected
	}

	// Generate transaction ID
	id := allocateId()

	// Create transaction
	tx := &CallbackTransaction{
		id:       id,
		conn:     c,
		state:    TxStateActive,
		callback: callback,
	}

	// Store transaction
	c.mu.Lock()
	c.transactions[id] = tx
	c.mu.Unlock()

	// Create and send BEGIN frame
	beginFrame := frame.New(frame.BEGIN, frame.Transaction, id)
	writer := frame.NewUnwrapCbioWriter(c.conn)
	err := writer.WriteSync(beginFrame)
	if err != nil {
		// Remove transaction from map on error
		c.mu.Lock()
		delete(c.transactions, id)
		c.mu.Unlock()
		return nil, err
	}

	// Update statistics
	c.mu.Lock()
	c.stats.FramesSent++
	c.mu.Unlock()

	// Notify callback asynchronously
	if callback != nil {
		go callback(c, tx, TransactionBegan, nil)
	}

	return tx, nil
}

// Close initiates graceful shutdown with callback notification
func (c *CallbackConn) Close(shutdownCallback ShutdownCallback) error {
	currentState := c.GetState()
	if currentState == Disconnected {
		if shutdownCallback != nil {
			go shutdownCallback(c, ErrAlreadyClosed)
		}
		return ErrAlreadyClosed
	}
	if currentState == Disconnecting {
		return nil // Already shutting down
	}

	// Store shutdown callback
	c.mu.Lock()
	c.shutdownCallback = shutdownCallback
	c.mu.Unlock()

	// Clean up pending transactions
	c.cleanupTransactions()

	// Initiate disconnect process
	return c.Disconnect(func(conn *CallbackConn, err error) {
		// Call shutdown callback when disconnect completes
		c.mu.RLock()
		callback := c.shutdownCallback
		c.mu.RUnlock()

		if callback != nil {
			callback(conn, err)
		}
	})
}

// cleanupTransactions handles cleanup of pending transactions during shutdown
func (c *CallbackConn) cleanupTransactions() {
	c.mu.Lock()
	transactions := make([]*CallbackTransaction, 0, len(c.transactions))
	for _, tx := range c.transactions {
		transactions = append(transactions, tx)
	}
	// Clear the transactions map
	c.transactions = make(map[string]*CallbackTransaction)
	c.mu.Unlock()

	// Notify all pending transactions about shutdown
	shutdownErr := newErrorMessage("connection shutting down")
	for _, tx := range transactions {
		tx.state = TxStateAborted
		if tx.callback != nil {
			go tx.callback(c, tx, TransactionError, shutdownErr)
		}
	}
}
