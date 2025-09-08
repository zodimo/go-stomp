package stomp

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
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

func (f *FakeConn) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, errors.New("connection closed")
	}
	return f.reader.Read(p)
}

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
	conn := NewCallbackConn(client)

	c.Assert(conn, NotNil)
	c.Assert(conn.GetState(), Equals, Disconnected)
	c.Assert(conn.msgSendTimeout, Equals, DefaultMsgSendTimeout)
	c.Assert(conn.rcvReceiptTimeout, Equals, DefaultRcvReceiptTimeout)
	c.Assert(conn.disconnectReceiptTimeout, Equals, DefaultDisconnectReceiptTimeout)
}

func (s *CallbackConnSuite) TestCallbackConnConnect(c *C) {
	client, server := NewFakeConn()
	conn := NewCallbackConn(client)

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
	client, server := NewFakeConn()
	conn := NewCallbackConn(client)

	var disconnectResult struct {
		err    error
		called bool
	}

	// Set up disconnect callback
	disconnectCallback := func(c *CallbackConn, err error) {
		disconnectResult.err = err
		disconnectResult.called = true
	}

	// First connect
	conn.setState(Connected)

	// Start disconnect in goroutine
	go func() {
		err := conn.Disconnect(disconnectCallback)
		c.Assert(err, IsNil)
	}()

	// Simulate server response
	go func() {
		reader := frame.NewReader(server)
		writer := frame.NewWriter(server)

		// Read DISCONNECT frame
		disconnectFrame, err := reader.Read()
		c.Assert(err, IsNil)
		c.Assert(disconnectFrame.Command, Equals, frame.DISCONNECT)

		// Get receipt ID
		receiptId := disconnectFrame.Header.Get(frame.Receipt)
		c.Assert(receiptId, Not(Equals), "")

		// Send RECEIPT response
		receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
		err = writer.Write(receiptFrame)
		c.Assert(err, IsNil)
	}()

	// Wait for disconnect to complete
	time.Sleep(100 * time.Millisecond)

	c.Assert(conn.GetState(), Equals, Disconnected)
	c.Assert(disconnectResult.called, Equals, true)
	c.Assert(disconnectResult.err, IsNil)
}

func (s *CallbackConnSuite) TestCallbackConnStateChange(c *C) {
	client, _ := NewFakeConn()
	conn := NewCallbackConn(client)

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
	conn := NewCallbackConn(client)

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
	conn := NewCallbackConn(client)

	// Set state to connected
	conn.setState(Connected)

	// Try to connect again
	err := conn.Connect(nil)
	c.Assert(err, Equals, ErrAlreadyConnected)
}

func (s *CallbackConnSuite) TestCallbackConnDisconnectAlreadyDisconnected(c *C) {
	client, _ := NewFakeConn()
	conn := NewCallbackConn(client)

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
