package stomp

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
	"github.com/zodimo/go-netkit/cbio"
	. "gopkg.in/check.v1"
)

type CallbackConnSuite struct{}

var _ = Suite(&CallbackConnSuite{})

// FakeConn implements io.ReadWriteCloser for testing
type FakeConn struct {
	reader io.ReadCloser
	writer io.WriteCloser
	closed bool
	mu     sync.Mutex
}

func NewFakeConn() (client *FakeConn, server *FakeConn) {
	clientReader, serverWriter := io.Pipe()
	serverReader, clientWriter := io.Pipe()

	client = &FakeConn{
		reader: clientReader,
		writer: clientWriter,
	}

	server = &FakeConn{
		reader: serverReader,
		writer: serverWriter,
	}

	return client, server
}

// Implement io.Reader interface
func (f *FakeConn) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, errors.New("connection closed")
	}
	return f.reader.Read(p)
}

// Implement io.Writer interface
func (f *FakeConn) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, errors.New("connection closed")
	}
	return f.writer.Write(p)
}

func (f *FakeConn) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true

	var err1, err2 error
	if f.reader != nil {
		err1 = f.reader.Close()
	}
	if f.writer != nil {
		err2 = f.writer.Close()
	}

	if err1 != nil {
		return err1
	}
	return err2
}

func (s *CallbackConnSuite) TestNewCallbackConn(c *C) {
	client, _ := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	c.Assert(conn, NotNil)
	c.Assert(conn.GetState(), Equals, Disconnected)
	c.Assert(conn.msgSendTimeout, Equals, DefaultMsgSendTimeout)
	c.Assert(conn.rcvReceiptTimeout, Equals, DefaultRcvReceiptTimeout)
	c.Assert(conn.disconnectReceiptTimeout, Equals, DefaultDisconnectReceiptTimeout)

	// Verify frame reader is initialized but not running
	stats := conn.GetConnectionStats()
	c.Assert(stats.FrameReaderRunning, Equals, false)
	c.Assert(conn.frameChannel, NotNil)
	c.Assert(conn.errorChannel, NotNil)
}

func (s *CallbackConnSuite) TestFrameReaderStartStop(c *C) {
	client, _ := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	// Initially frame reader should not be running
	stats := conn.GetConnectionStats()
	c.Assert(stats.FrameReaderRunning, Equals, false)

	// Start frame reader
	conn.startFrameReader()

	// Verify frame reader is running
	stats = conn.GetConnectionStats()
	c.Assert(stats.FrameReaderRunning, Equals, true)
	c.Assert(stats.FrameReaderStartedAt.IsZero(), Equals, false)

	// Stop frame reader
	conn.stopFrameReader()

	// Verify frame reader is stopped
	stats = conn.GetConnectionStats()
	c.Assert(stats.FrameReaderRunning, Equals, false)
}

func (s *CallbackConnSuite) TestFrameReaderDoubleStart(c *C) {
	client, _ := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	// Start frame reader twice
	conn.startFrameReader()
	conn.startFrameReader() // Should be no-op

	// Verify only one reader is running
	stats := conn.GetConnectionStats()
	c.Assert(stats.FrameReaderRunning, Equals, true)

	// Stop should work normally
	conn.stopFrameReader()
	stats = conn.GetConnectionStats()
	c.Assert(stats.FrameReaderRunning, Equals, false)
}

func (s *CallbackConnSuite) TestFrameReaderDoubleStop(c *C) {
	client, _ := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	// Stop without starting (should be no-op)
	conn.stopFrameReader()

	// Start and then stop twice
	conn.startFrameReader()
	conn.stopFrameReader()
	conn.stopFrameReader() // Should be no-op

	stats := conn.GetConnectionStats()
	c.Assert(stats.FrameReaderRunning, Equals, false)
}

func (s *CallbackConnSuite) TestFrameReaderIntegrationWithConnection(c *C) {
	client, server := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	// Set up callbacks to track events
	var connectedCalled bool

	conn.SetErrorCallback(func(c *CallbackConn, err error) {
		// Error callback for connection issues
	})

	// Start connection process
	err := conn.Connect(func(c *CallbackConn, session, server string, version Version) {
		connectedCalled = true
	})
	c.Assert(err, IsNil)

	// Verify frame reader is started during connection
	time.Sleep(50 * time.Millisecond) // Give time for connection process to start
	stats := conn.GetConnectionStats()
	c.Assert(stats.FrameReaderRunning, Equals, true)

	// Send CONNECTED response from server
	connectedFrame := frame.New(frame.CONNECTED, frame.Version, string(V12), frame.Session, "test-session")
	writer := frame.NewWriter(server)
	err = writer.Write(connectedFrame)
	c.Assert(err, IsNil)

	// Wait for connection to complete
	time.Sleep(100 * time.Millisecond)
	c.Assert(conn.GetState(), Equals, Connected)
	c.Assert(connectedCalled, Equals, true)

	// Verify frame reader is still running after successful connection
	stats = conn.GetConnectionStats()
	c.Assert(stats.FrameReaderRunning, Equals, true)

	// Close connection
	server.Close()
	client.Close()
}

func (s *CallbackConnSuite) TestFrameReaderErrorHandling(c *C) {
	client, server := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	var errorCalled bool
	var lastError error

	conn.SetErrorCallback(func(c *CallbackConn, err error) {
		errorCalled = true
		lastError = err
	})

	// Start connection process
	err := conn.Connect(func(c *CallbackConn, session, server string, version Version) {
		// Should not be called due to connection error
	})
	c.Assert(err, IsNil)

	// Wait for frame reader to start
	time.Sleep(50 * time.Millisecond)

	// Close server side to cause read error
	server.Close()

	// Wait for error to propagate
	time.Sleep(100 * time.Millisecond)

	c.Assert(errorCalled, Equals, true)
	c.Assert(lastError, NotNil)
	c.Assert(conn.GetState(), Equals, Disconnected)

	// Verify frame reader is stopped after error
	stats := conn.GetConnectionStats()
	c.Assert(stats.FrameReaderRunning, Equals, false)
}

func (s *CallbackConnSuite) TestFrameReaderChannelMetrics(c *C) {
	client, _ := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	// Check initial channel sizes
	stats := conn.GetConnectionStats()
	c.Assert(stats.FrameChannelSize, Equals, 0)
	c.Assert(stats.ErrorChannelSize, Equals, 0)

	// Start frame reader
	conn.startFrameReader()

	// Channel sizes should still be 0 initially
	stats = conn.GetConnectionStats()
	c.Assert(stats.FrameChannelSize, Equals, 0)
	c.Assert(stats.ErrorChannelSize, Equals, 0)
	c.Assert(stats.FrameReaderRunning, Equals, true)

	// Stop frame reader
	conn.stopFrameReader()
}

func (s *CallbackConnSuite) TestCallbackConnConnect(c *C) {
	client, server := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	var connectResult struct {
		session string
		server  string
		version Version
		called  bool
	}

	// Set up connect callback
	connectCallback := func(c *CallbackConn, session string, srv string, version Version) {
		connectResult.session = session
		connectResult.server = srv
		connectResult.version = version
		connectResult.called = true
	}

	// Start connection in goroutine
	go func() {
		err := conn.Connect(connectCallback)
		c.Assert(err, IsNil)
	}()

	// Simulate server response
	go func() {
		reader := frame.NewReader(server)
		writer := frame.NewWriter(server)

		// Read CONNECT frame
		connectFrame, err := reader.Read()
		c.Assert(err, IsNil)
		c.Assert(connectFrame.Command, Equals, frame.CONNECT)

		// Send CONNECTED response
		connectedFrame := frame.New(frame.CONNECTED,
			frame.Version, "1.2",
			frame.Server, "test-server",
			frame.Session, "test-session")

		err = writer.Write(connectedFrame)
		c.Assert(err, IsNil)
	}()

	// Wait for connection to complete
	time.Sleep(100 * time.Millisecond)

	c.Assert(conn.GetState(), Equals, Connected)
	c.Assert(connectResult.called, Equals, true)
	c.Assert(connectResult.version, Equals, V12)
	c.Assert(connectResult.server, Equals, "test-server")
	c.Assert(connectResult.session, Equals, "test-session")
}

func (s *CallbackConnSuite) TestCallbackConnDisconnect(c *C) {
	// This is a simplified test that just checks the state transitions
	client, _ := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	// Set initial state
	conn.setState(Connected)

	// Set up disconnect callback
	disconnectCalled := false
	conn.disconnectCallback = func(c *CallbackConn, err error) {
		disconnectCalled = true
	}

	// Directly call finalizeDisconnect
	conn.finalizeDisconnect(nil)

	// Verify the state is now Disconnected
	c.Assert(conn.GetState(), Equals, Disconnected)
	c.Assert(disconnectCalled, Equals, true)
}

func (s *CallbackConnSuite) TestCallbackConnStateChange(c *C) {
	client, _ := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	var stateChanges []struct {
		oldState ConnectionState
		newState ConnectionState
	}

	// Set up state change callback
	stateChangeCallback := func(c *CallbackConn, oldState, newState ConnectionState) {
		stateChanges = append(stateChanges, struct {
			oldState ConnectionState
			newState ConnectionState
		}{oldState, newState})
	}

	conn.SetStateChangeCallback(stateChangeCallback)

	// Test state changes
	conn.setState(Connecting)
	conn.setState(Connected)
	conn.setState(Disconnecting)
	conn.setState(Disconnected)

	c.Assert(len(stateChanges), Equals, 4)
	c.Assert(stateChanges[0].oldState, Equals, Disconnected)
	c.Assert(stateChanges[0].newState, Equals, Connecting)
	c.Assert(stateChanges[1].oldState, Equals, Connecting)
	c.Assert(stateChanges[1].newState, Equals, Connected)
	c.Assert(stateChanges[2].oldState, Equals, Connected)
	c.Assert(stateChanges[2].newState, Equals, Disconnecting)
	c.Assert(stateChanges[3].oldState, Equals, Disconnecting)
	c.Assert(stateChanges[3].newState, Equals, Disconnected)
}

func (s *CallbackConnSuite) TestCallbackConnError(c *C) {
	client, _ := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	var errorResult struct {
		err    error
		called bool
	}

	// Set up error callback
	errorCallback := func(c *CallbackConn, err error) {
		errorResult.err = err
		errorResult.called = true
	}

	conn.SetErrorCallback(errorCallback)

	// Test error notification
	testErr := errors.New("test error")
	conn.notifyError(testErr)

	c.Assert(errorResult.called, Equals, true)
	c.Assert(errorResult.err, Equals, testErr)
}

func (s *CallbackConnSuite) TestCallbackConnConnectAlreadyConnected(c *C) {
	client, _ := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	// Set state to connected
	conn.setState(Connected)

	// Try to connect again
	err := conn.Connect(nil)
	c.Assert(err, Equals, ErrAlreadyConnected)
}

func (s *CallbackConnSuite) TestCallbackConnDisconnectAlreadyDisconnected(c *C) {
	client, _ := NewFakeConn()
	cbioClient := cbio.WrapReadWriteCloser(client)
	conn := NewCallbackConn(cbioClient)

	// Connection is already disconnected by default
	err := conn.Disconnect(nil)
	c.Assert(err, Equals, ErrAlreadyClosed)
}

func (s *CallbackConnSuite) TestConnectionStateString(c *C) {
	c.Assert(Disconnected.String(), Equals, "Disconnected")
	c.Assert(Connecting.String(), Equals, "Connecting")
	c.Assert(Connected.String(), Equals, "Connected")
	c.Assert(Disconnecting.String(), Equals, "Disconnecting")

	// Test unknown state
	unknownState := ConnectionState(999)
	c.Assert(unknownState.String(), Equals, "Unknown")
}
