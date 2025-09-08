package stomp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
	"github.com/go-stomp/stomp/v3/internal/log"
	"github.com/zodimo/go-netkit/cbio"
)

// PendingOperation represents an operation waiting for a response
type PendingOperation struct {
	Type       string             // "connect", "send", "subscribe", etc.
	ReceiptID  string             // Receipt ID to match
	ResponseCh chan *frame.Frame  // Channel to send response
	ErrorCh    chan error         // Channel to send errors
	Context    context.Context    // For timeout handling
	Cancel     context.CancelFunc // Cancel function
}

// FrameRouter handles incoming frames and routes them to appropriate handlers
type FrameRouter struct {
	pendingOps    map[string]*PendingOperation
	subscriptions map[string]*CallbackSubscription
	mu            sync.RWMutex
	stopCh        chan struct{}
	conn          *CallbackConn // Reference to parent connection
}

// NewFrameRouter creates a new frame router
func NewFrameRouter(conn *CallbackConn) *FrameRouter {
	return &FrameRouter{
		pendingOps:    make(map[string]*PendingOperation),
		subscriptions: make(map[string]*CallbackSubscription),
		stopCh:        make(chan struct{}),
		conn:          conn,
	}
}

// RegisterPendingOperation registers an operation waiting for a receipt
func (r *FrameRouter) RegisterPendingOperation(op *PendingOperation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pendingOps[op.ReceiptID] = op
}

// UnregisterPendingOperation removes a pending operation
func (r *FrameRouter) UnregisterPendingOperation(receiptID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if op, exists := r.pendingOps[receiptID]; exists {
		if op.Cancel != nil {
			op.Cancel()
		}
		delete(r.pendingOps, receiptID)
	}
}

// RegisterSubscription registers a subscription for message routing
func (r *FrameRouter) RegisterSubscription(subscriptionID string, sub *CallbackSubscription) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subscriptions[subscriptionID] = sub
}

// UnregisterSubscription removes a subscription
func (r *FrameRouter) UnregisterSubscription(subscriptionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.subscriptions, subscriptionID)
}

// RouteFrame routes an incoming frame to the appropriate handler
func (r *FrameRouter) RouteFrame(f *frame.Frame) {
	switch f.Command {
	case frame.RECEIPT:
		r.routeReceiptFrame(f)
	case frame.MESSAGE:
		r.routeMessageFrame(f)
	case frame.ERROR:
		r.routeErrorFrame(f)
	case frame.CONNECTED:
		r.routeConnectedFrame(f)
	default:
		r.routeUnknownFrame(f)
	}
}

// routeReceiptFrame routes RECEIPT frames to pending operations
func (r *FrameRouter) routeReceiptFrame(f *frame.Frame) {
	receiptID := f.Header.Get(frame.ReceiptId)
	if receiptID == "" {
		r.conn.log.Error("Received RECEIPT frame without receipt-id")
		return
	}

	r.mu.Lock()
	op, exists := r.pendingOps[receiptID]
	if exists {
		delete(r.pendingOps, receiptID)
	}
	r.mu.Unlock()

	if exists {
		select {
		case op.ResponseCh <- f:
			// Receipt delivered successfully
		default:
			// Channel full or closed, log warning
			r.conn.log.Warning("Failed to deliver RECEIPT frame to pending operation")
		}
	} else {
		r.conn.log.Warning("Received RECEIPT frame for unknown receipt-id: " + receiptID)
	}
}

// routeMessageFrame routes MESSAGE frames to subscription handlers
func (r *FrameRouter) routeMessageFrame(f *frame.Frame) {
	// Delegate to existing handleMessageFrame method
	r.conn.handleMessageFrame(f)
}

// routeErrorFrame routes ERROR frames to error callbacks
func (r *FrameRouter) routeErrorFrame(f *frame.Frame) {
	// Check if this error is for a pending operation
	receiptID := f.Header.Get(frame.ReceiptId)
	if receiptID != "" {
		r.mu.Lock()
		op, exists := r.pendingOps[receiptID]
		if exists {
			delete(r.pendingOps, receiptID)
		}
		r.mu.Unlock()

		if exists {
			select {
			case op.ErrorCh <- newError(f):
				return
			default:
				// Channel full or closed, fall through to general error handling
			}
		}
	}

	// General error handling
	r.conn.handleConnectionError(newError(f))
}

// routeConnectedFrame routes CONNECTED frames to connection handlers
func (r *FrameRouter) routeConnectedFrame(f *frame.Frame) {
	// This would typically be handled during connection establishment
	// For now, log it as unexpected since CONNECTED should only occur during connect
	r.conn.log.Warning("Received unexpected CONNECTED frame")
}

// routeUnknownFrame handles unknown frame types gracefully
func (r *FrameRouter) routeUnknownFrame(f *frame.Frame) {
	r.conn.log.Warning("Received unknown frame type: " + f.Command)
}

// Stop stops the frame router and cancels all pending operations
func (r *FrameRouter) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Cancel all pending operations
	for _, op := range r.pendingOps {
		if op.Cancel != nil {
			op.Cancel()
		}
		select {
		case op.ErrorCh <- ErrClosedUnexpectedly:
		default:
			// Channel closed or full
		}
	}

	// Clear maps
	r.pendingOps = make(map[string]*PendingOperation)
	r.subscriptions = make(map[string]*CallbackSubscription)

	// Signal stop
	select {
	case r.stopCh <- struct{}{}:
	default:
	}
}

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

	// Logging
	log Logger

	// Background frame reader
	readerRunning   bool
	readerStartedAt time.Time
	readerDone      chan struct{}
	frameChannel    chan *frame.Frame
	errorChannel    chan error

	// Frame router for centralized frame dispatching
	frameRouter *FrameRouter
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
		// Initialize frame reader channels
		frameChannel: make(chan *frame.Frame, 100), // Buffered channel for frames
		errorChannel: make(chan error, 10),         // Buffered channel for errors
	}

	// Initialize frame router
	c.frameRouter = NewFrameRouter(c)

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

	// Set logger from options (defaults to StdLogger)
	if c.options.Logger != nil {
		c.log = c.options.Logger
	} else {
		c.log = log.StdLogger{}
	}

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

	stats := c.stats
	// Add frame reader metrics
	stats.FrameReaderRunning = c.readerRunning
	stats.FrameReaderStartedAt = c.readerStartedAt
	stats.FrameChannelSize = len(c.frameChannel)
	stats.ErrorChannelSize = len(c.errorChannel)

	return stats
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

// handleConnectionError handles connection errors with proper cleanup
func (c *CallbackConn) handleConnectionError(err error) {
	// Stop frame reader if running
	c.stopFrameReader()

	// Set state to disconnected
	c.setState(Disconnected)

	// Notify error callback
	c.notifyError(err)
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

// startFrameReader starts the background frame reader goroutine
func (c *CallbackConn) startFrameReader() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Don't start if already running
	if c.readerRunning {
		if c.log != nil {
			c.log.Debug("frame reader already running, skipping start")
		}
		return
	}

	c.readerRunning = true
	c.readerStartedAt = time.Now()
	c.readerDone = make(chan struct{})

	if c.log != nil {
		c.log.Debug("starting background frame reader")
	}

	// Start the frame reader goroutine
	go c.frameReaderLoop()
}

// stopFrameReader stops the background frame reader goroutine
func (c *CallbackConn) stopFrameReader() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.readerRunning {
		if c.log != nil {
			c.log.Debug("frame reader not running, skipping stop")
		}
		return
	}

	if c.log != nil {
		c.log.Debug("stopping background frame reader")
	}

	c.readerRunning = false

	// Wait for reader to finish
	if c.readerDone != nil {
		<-c.readerDone
		c.readerDone = nil
		if c.log != nil {
			c.log.Debug("background frame reader stopped")
		}
	}
}

// frameReaderLoop runs the background frame reading loop
func (c *CallbackConn) frameReaderLoop() {
	reader := frame.NewUnwrapCbioReader(c.conn)
	defer close(c.readerDone)

	if c.log != nil {
		c.log.Debug("frame reader loop started")
	}

	// Create channels for async frame reading
	frameCh := make(chan *frame.Frame, 1)
	errorCh := make(chan error, 1)

	// Start async frame reading goroutine
	go func() {
		for {
			frame, err := reader.ReadSync()
			if err != nil {
				if c.log != nil {
					c.log.Debugf("frame reader error: %v", err)
				}
				errorCh <- err
				return
			}
			if c.log != nil {
				if frame == nil {
					c.log.Debug("received heart-beat frame")
				} else {
					c.log.Debugf("received frame: %s", frame.Command)
				}
			}
			frameCh <- frame
		}
	}()

	frameCount := 0
	for {
		state := c.GetState()
		// Stop reading if disconnected or disconnecting
		if state == Disconnected {
			if c.log != nil {
				c.log.Debugf("frame reader loop exiting, processed %d frames", frameCount)
			}
			return
		}

		select {
		case frame := <-frameCh:
			frameCount++
			// Send frame to frame channel if connection is still active
			currentState := c.GetState()
			if currentState != Disconnected {
				select {
				case c.frameChannel <- frame:
				default:
					if c.log != nil {
						c.log.Warning("frame channel is full, dropping frame")
					}
				}
			}

		case err := <-errorCh:
			if c.log != nil {
				c.log.Errorf("frame reading failed: %v", err)
			}
			// Send error to error channel if connection is still active
			currentState := c.GetState()
			if currentState != Disconnected {
				select {
				case c.errorChannel <- err:
				default:
					if c.log != nil {
						c.log.Error("error channel is full, dropping error")
					}
				}
			}
			return

		case <-time.After(30 * time.Second):
			// Check if we should still be running every 30 seconds
			// This prevents the goroutine from hanging indefinitely
			if c.log != nil {
				c.log.Debugf("frame reader health check - processed %d frames", frameCount)
			}
			continue
		}
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
	// Create frame writer using the standard io adapter
	writer := frame.NewUnwrapCbioWriter(c.conn)

	// Send CONNECT frame
	err := writer.WriteSync(connectFrame)
	if err != nil {
		c.handleConnectionError(err)
		return
	}

	// Start temporary frame reader for connection process
	c.startFrameReader()

	// Wait for CONNECTED response with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		select {
		case response := <-c.frameChannel:
			if response != nil && response.Command == frame.CONNECTED {
				c.handleConnectResponse(response)
				return
			} else if response != nil && response.Command == frame.ERROR {
				c.handleConnectionError(newError(response))
				return
			}
			// Continue waiting for CONNECTED frame, ignore other frames

		case err := <-c.errorChannel:
			c.handleConnectionError(err)
			return

		case <-ctx.Done():
			c.handleConnectionError(ctx.Err())
			return
		}
	}
}

// handleConnectResponse processes the CONNECTED frame response
func (c *CallbackConn) handleConnectResponse(response *frame.Frame) {
	if response.Command != frame.CONNECTED {
		err := newError(response)
		c.handleConnectionError(err)
		return
	}

	// Extract connection details from CONNECTED frame
	c.server = response.Header.Get(frame.Server)
	c.session = response.Header.Get(frame.Session)

	// Handle version negotiation
	if versionString := response.Header.Get(frame.Version); versionString != "" {
		version := Version(versionString)
		if err := version.CheckSupported(); err != nil {
			c.handleConnectionError(Error{
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
			c.handleConnectionError(Error{
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

	// Start message processing loop (frame reader already started in performConnect)
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
	// Create frame writer using the standard io adapter
	writer := frame.NewUnwrapCbioWriter(c.conn)

	// Create DISCONNECT frame with receipt
	receiptId := allocateId()
	disconnectFrame := frame.New(frame.DISCONNECT, frame.Receipt, receiptId)

	// Send DISCONNECT frame
	err := writer.WriteSync(disconnectFrame)
	if err != nil {
		c.finalizeDisconnect(err)
		return
	}

	// Create pending operation for receipt tracking
	ctx, cancel := context.WithTimeout(context.Background(), c.disconnectReceiptTimeout)
	defer cancel()

	responseCh := make(chan *frame.Frame, 1)
	errorCh := make(chan error, 1)

	pendingOp := &PendingOperation{
		Type:       "disconnect",
		ReceiptID:  receiptId,
		ResponseCh: responseCh,
		ErrorCh:    errorCh,
		Context:    ctx,
		Cancel:     cancel,
	}

	// Register pending operation with frame router
	c.frameRouter.RegisterPendingOperation(pendingOp)

	// Wait for RECEIPT response with timeout
	select {
	case <-responseCh:
		// Receipt received, disconnect successful
		c.finalizeDisconnect(nil)
	case err := <-errorCh:
		// Error occurred
		c.finalizeDisconnect(err)
	case <-ctx.Done():
		// Timeout occurred, unregister the operation
		c.frameRouter.UnregisterPendingOperation(receiptId)
		c.finalizeDisconnect(ErrDisconnectReceiptTimeout)
	}
}

// finalizeDisconnect completes the disconnection process
func (c *CallbackConn) finalizeDisconnect(err error) {
	// Stop the background frame reader
	c.stopFrameReader()

	// Stop the frame router
	c.frameRouter.Stop()

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

	// Main processing loop with heart-beat monitoring
	for c.GetState() == Connected {
		select {
		case <-readTimeoutChannel:
			// Read timeout - heart-beat not received in time
			c.notifyHeartBeat(HeartBeatTimeout, ErrClosedUnexpectedly)
			c.setHealthStatus(HealthUnhealthy)
			c.handleConnectionError(newErrorMessage("read timeout"))
			return

		case <-writeTimeoutChannel:
			// Write timeout - send heart-beat frame
			err := writer.WriteSync(nil)
			if err != nil {
				c.handleConnectionError(err)
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

		case err := <-c.errorChannel:
			// Error from background frame reader
			c.handleConnectionError(err)
			return

		case f := <-c.frameChannel:
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

			// Process non-heart-beat frames using frame router
			c.frameRouter.RouteFrame(f)
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
