package stomp

import (
	"sync"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
	. "gopkg.in/check.v1"
)

type CallbackMessagingSuite struct{}

var _ = Suite(&CallbackMessagingSuite{})

func (s *CallbackMessagingSuite) TestSendWithCallback(c *C) {
	client, server := NewFakeConn()
	defer client.Close()
	defer server.Close()

	conn := NewCallbackConn(client)

	// Set up connection state
	conn.setState(Connected)
	conn.version = V12

	var callbackCalled bool
	var callbackErr error
	var callbackDestination string
	var wg sync.WaitGroup
	wg.Add(1)

	// Test successful send
	err := conn.Send("/queue/test", "text/plain", []byte("test message"), func(c *CallbackConn, dest string, err error) {
		callbackCalled = true
		callbackErr = err
		callbackDestination = dest
		wg.Done()
	})

	c.Assert(err, IsNil)

	// Read the frame from server side
	reader := frame.NewReader(server)
	f, err := reader.Read()
	c.Assert(err, IsNil)
	c.Assert(f.Command, Equals, frame.SEND)
	c.Assert(f.Header.Get(frame.Destination), Equals, "/queue/test")
	c.Assert(f.Header.Get(frame.ContentType), Equals, "text/plain")
	c.Assert(string(f.Body), Equals, "test message")

	// Wait for callback
	wg.Wait()

	c.Assert(callbackCalled, Equals, true)
	c.Assert(callbackErr, IsNil)
	c.Assert(callbackDestination, Equals, "/queue/test")
}

func (s *CallbackMessagingSuite) TestSendWithReceiptCallback(c *C) {
	client, server := NewFakeConn()
	defer client.Close()
	defer server.Close()

	conn := NewCallbackConn(client)

	// Set up connection state
	conn.setState(Connected)
	conn.version = V12

	var callbackCalled bool
	var callbackErr error
	var wg sync.WaitGroup
	wg.Add(1)

	// Start goroutine to send receipt response
	go func() {
		time.Sleep(10 * time.Millisecond) // Small delay to ensure send is processed

		reader := frame.NewReader(server)
		f, err := reader.Read()
		c.Assert(err, IsNil)
		c.Assert(f.Command, Equals, frame.SEND)

		receiptId := f.Header.Get(frame.Receipt)
		c.Assert(receiptId, Not(Equals), "")

		// Send receipt response
		writer := frame.NewWriter(server)
		receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
		err = writer.Write(receiptFrame)
		c.Assert(err, IsNil)
	}()

	// Test send with receipt
	err := conn.SendWithReceipt("/queue/test", "text/plain", []byte("test message"), func(c *CallbackConn, dest string, err error) {
		callbackCalled = true
		callbackErr = err
		wg.Done()
	}, 5*time.Second)

	c.Assert(err, IsNil)

	// Wait for callback
	wg.Wait()

	c.Assert(callbackCalled, Equals, true)
	c.Assert(callbackErr, IsNil)
}

func (s *CallbackMessagingSuite) TestSendReceiptTimeout(c *C) {
	client, server := NewFakeConn()
	defer client.Close()
	defer server.Close()

	conn := NewCallbackConn(client)

	// Set up connection state
	conn.setState(Connected)
	conn.version = V12

	var callbackCalled bool
	var callbackErr error
	var wg sync.WaitGroup
	wg.Add(1)

	// Don't send receipt response - let it timeout

	// Test send with receipt timeout
	err := conn.SendWithReceipt("/queue/test", "text/plain", []byte("test message"), func(c *CallbackConn, dest string, err error) {
		callbackCalled = true
		callbackErr = err
		wg.Done()
	}, 100*time.Millisecond)

	c.Assert(err, IsNil)

	// Wait for callback
	wg.Wait()

	c.Assert(callbackCalled, Equals, true)
	c.Assert(callbackErr, Equals, ErrSendReceiptTimeout)
}

func (s *CallbackMessagingSuite) TestSubscribeWithCallback(c *C) {
	client, server := NewFakeConn()
	defer client.Close()
	defer server.Close()

	conn := NewCallbackConn(client)

	// Set up connection state
	conn.setState(Connected)
	conn.version = V12

	var subscriptionCallback *CallbackSubscription
	var subscriptionEvent SubscriptionEvent
	var subscriptionErr error
	var wg sync.WaitGroup
	wg.Add(2) // Expecting SubscriptionCreated and SubscriptionActive events

	messageHandler := func(c *CallbackConn, msg *CallbackMessage) {
		// Message handler implementation
	}

	// Test successful subscription
	sub, err := conn.Subscribe("/queue/test", AckClient, messageHandler, func(c *CallbackConn, sub *CallbackSubscription, event SubscriptionEvent, err error) {
		subscriptionCallback = sub
		subscriptionEvent = event
		subscriptionErr = err
		wg.Done()
	})

	c.Assert(err, IsNil)
	c.Assert(sub, NotNil)
	c.Assert(sub.Destination(), Equals, "/queue/test")
	c.Assert(sub.AckMode(), Equals, AckClient)

	// Read the frame from server side
	reader := frame.NewReader(server)
	f, err := reader.Read()
	c.Assert(err, IsNil)
	c.Assert(f.Command, Equals, frame.SUBSCRIBE)
	c.Assert(f.Header.Get(frame.Destination), Equals, "/queue/test")
	c.Assert(f.Header.Get(frame.Ack), Equals, "client")

	// Wait for callbacks
	wg.Wait()

	c.Assert(subscriptionCallback, NotNil)
	c.Assert(subscriptionErr, IsNil)
	c.Assert(subscriptionEvent, Equals, SubscriptionActive) // Last event received
	c.Assert(sub.IsActive(), Equals, true)
}

func (s *CallbackMessagingSuite) TestUnsubscribeWithCallback(c *C) {
	client, server := NewFakeConn()
	defer client.Close()
	defer server.Close()

	conn := NewCallbackConn(client)

	// Set up connection state
	conn.setState(Connected)
	conn.version = V12

	// First create a subscription
	messageHandler := func(c *CallbackConn, msg *CallbackMessage) {}
	sub, err := conn.Subscribe("/queue/test", AckClient, messageHandler, nil)
	c.Assert(err, IsNil)

	// Read and ignore the SUBSCRIBE frame
	reader := frame.NewReader(server)
	_, err = reader.Read()
	c.Assert(err, IsNil)

	// Mark subscription as active
	sub.active = true

	var callbackCalled bool
	var callbackEvent SubscriptionEvent
	var wg sync.WaitGroup
	wg.Add(1)

	// Start goroutine to send receipt response
	go func() {
		time.Sleep(10 * time.Millisecond)

		f, err := reader.Read()
		c.Assert(err, IsNil)
		c.Assert(f.Command, Equals, frame.UNSUBSCRIBE)

		receiptId := f.Header.Get(frame.Receipt)
		c.Assert(receiptId, Not(Equals), "")

		// Send receipt response
		writer := frame.NewWriter(server)
		receiptFrame := frame.New(frame.RECEIPT, frame.ReceiptId, receiptId)
		err = writer.Write(receiptFrame)
		c.Assert(err, IsNil)
	}()

	// Test unsubscribe
	err = conn.Unsubscribe(sub, func(c *CallbackConn, sub *CallbackSubscription, event SubscriptionEvent, err error) {
		callbackCalled = true
		callbackEvent = event
		wg.Done()
	})

	c.Assert(err, IsNil)

	// Wait for callback
	wg.Wait()

	c.Assert(callbackCalled, Equals, true)
	c.Assert(callbackEvent, Equals, SubscriptionUnsubscribed)
	c.Assert(sub.IsActive(), Equals, false)
}

func (s *CallbackMessagingSuite) TestMessageHandlerInvocation(c *C) {
	client, server := NewFakeConn()
	defer client.Close()
	defer server.Close()

	conn := NewCallbackConn(client)

	// Set up connection state
	conn.setState(Connected)
	conn.version = V12

	var receivedMessage *CallbackMessage
	var wg sync.WaitGroup
	wg.Add(1)

	messageHandler := func(c *CallbackConn, msg *CallbackMessage) {
		receivedMessage = msg
		wg.Done()
	}

	// Create subscription
	sub, err := conn.Subscribe("/queue/test", AckClient, messageHandler, nil)
	c.Assert(err, IsNil)

	// Read and ignore the SUBSCRIBE frame
	reader := frame.NewReader(server)
	_, err = reader.Read()
	c.Assert(err, IsNil)

	// Mark subscription as active
	sub.active = true

	// Start message processing in background
	go conn.startMessageProcessing()

	// Send a MESSAGE frame from server
	time.Sleep(10 * time.Millisecond) // Allow processing to start
	writer := frame.NewWriter(server)
	messageFrame := frame.New(frame.MESSAGE,
		frame.Destination, "/queue/test",
		frame.Subscription, sub.Id(),
		frame.MessageId, "msg-123",
		frame.ContentType, "text/plain")
	messageFrame.Body = []byte("test message body")

	err = writer.Write(messageFrame)
	c.Assert(err, IsNil)

	// Wait for message handler
	wg.Wait()

	c.Assert(receivedMessage, NotNil)
	c.Assert(receivedMessage.Destination, Equals, "/queue/test")
	c.Assert(receivedMessage.ContentType, Equals, "text/plain")
	c.Assert(string(receivedMessage.Body), Equals, "test message body")
	c.Assert(receivedMessage.Subscription.Id(), Equals, sub.Id())
	c.Assert(receivedMessage.ackId, Equals, "msg-123")
}

func (s *CallbackMessagingSuite) TestAckWithCallback(c *C) {
	client, server := NewFakeConn()
	defer client.Close()
	defer server.Close()

	conn := NewCallbackConn(client)

	// Set up connection state
	conn.setState(Connected)
	conn.version = V12

	// Create a test message
	sub := &CallbackSubscription{
		id:      "sub-123",
		ackMode: AckClient,
	}

	message := &CallbackMessage{
		Header:       frame.New(frame.MESSAGE, frame.MessageId, "msg-123").Header,
		Subscription: sub,
		Conn:         conn,
		ackId:        "msg-123",
	}

	var callbackCalled bool
	var callbackErr error
	var callbackMessageId string
	var wg sync.WaitGroup
	wg.Add(1)

	// Test ACK
	err := conn.Ack(message, func(c *CallbackConn, messageId string, err error) {
		callbackCalled = true
		callbackErr = err
		callbackMessageId = messageId
		wg.Done()
	})

	c.Assert(err, IsNil)

	// Read the frame from server side
	reader := frame.NewReader(server)
	f, err := reader.Read()
	c.Assert(err, IsNil)
	c.Assert(f.Command, Equals, frame.ACK)
	c.Assert(f.Header.Get(frame.MessageId), Equals, "msg-123")

	// Wait for callback
	wg.Wait()

	c.Assert(callbackCalled, Equals, true)
	c.Assert(callbackErr, IsNil)
	c.Assert(callbackMessageId, Equals, "msg-123")
}

func (s *CallbackMessagingSuite) TestNackWithCallback(c *C) {
	client, server := NewFakeConn()
	defer client.Close()
	defer server.Close()

	conn := NewCallbackConn(client)

	// Set up connection state
	conn.setState(Connected)
	conn.version = V12 // NACK supported in 1.2

	// Create a test message
	sub := &CallbackSubscription{
		id:      "sub-123",
		ackMode: AckClient,
	}

	message := &CallbackMessage{
		Header:       frame.New(frame.MESSAGE, frame.MessageId, "msg-123").Header,
		Subscription: sub,
		Conn:         conn,
		ackId:        "msg-123",
	}

	var callbackCalled bool
	var callbackErr error
	var wg sync.WaitGroup
	wg.Add(1)

	// Test NACK
	err := conn.Nack(message, func(c *CallbackConn, messageId string, err error) {
		callbackCalled = true
		callbackErr = err
		wg.Done()
	})

	c.Assert(err, IsNil)

	// Read the frame from server side
	reader := frame.NewReader(server)
	f, err := reader.Read()
	c.Assert(err, IsNil)
	c.Assert(f.Command, Equals, frame.NACK)
	c.Assert(f.Header.Get(frame.MessageId), Equals, "msg-123")

	// Wait for callback
	wg.Wait()

	c.Assert(callbackCalled, Equals, true)
	c.Assert(callbackErr, IsNil)
}

func (s *CallbackMessagingSuite) TestNackNotSupportedInV10(c *C) {
	client, server := NewFakeConn()
	defer client.Close()
	defer server.Close()

	conn := NewCallbackConn(client)

	// Set up connection state
	conn.setState(Connected)
	conn.version = V10 // NACK not supported in 1.0

	// Create a test message
	sub := &CallbackSubscription{
		id:      "sub-123",
		ackMode: AckClient,
	}

	message := &CallbackMessage{
		Header:       frame.New(frame.MESSAGE, frame.MessageId, "msg-123").Header,
		Subscription: sub,
		Conn:         conn,
		ackId:        "msg-123",
	}

	var callbackCalled bool
	var callbackErr error
	var wg sync.WaitGroup
	wg.Add(1)

	// Test NACK should fail
	err := conn.Nack(message, func(c *CallbackConn, messageId string, err error) {
		callbackCalled = true
		callbackErr = err
		wg.Done()
	})

	c.Assert(err, Equals, ErrNackNotSupported)

	// Wait for callback
	wg.Wait()

	c.Assert(callbackCalled, Equals, true)
	c.Assert(callbackErr, Equals, ErrNackNotSupported)
}

func (s *CallbackMessagingSuite) TestAckAutoMode(c *C) {
	client, server := NewFakeConn()
	defer client.Close()
	defer server.Close()

	conn := NewCallbackConn(client)

	// Set up connection state
	conn.setState(Connected)
	conn.version = V12

	// Create a test message with AckAuto
	sub := &CallbackSubscription{
		id:      "sub-123",
		ackMode: AckAuto,
	}

	message := &CallbackMessage{
		Header:       frame.New(frame.MESSAGE, frame.MessageId, "msg-123").Header,
		Subscription: sub,
		Conn:         conn,
		ackId:        "msg-123",
	}

	var callbackCalled bool
	var callbackErr error
	var wg sync.WaitGroup
	wg.Add(1)

	// Test ACK with auto mode - should not send frame but callback should be called
	err := conn.Ack(message, func(c *CallbackConn, messageId string, err error) {
		callbackCalled = true
		callbackErr = err
		wg.Done()
	})

	c.Assert(err, IsNil)

	// Wait for callback
	wg.Wait()

	c.Assert(callbackCalled, Equals, true)
	c.Assert(callbackErr, IsNil)

	// Should not have sent any frame to server
	// Use a timeout channel to check if no frame was sent
	done := make(chan bool, 1)
	go func() {
		reader := frame.NewReader(server)
		_, err := reader.Read()
		if err != nil {
			done <- false // Error reading (expected)
		} else {
			done <- true // Frame received (unexpected)
		}
	}()

	select {
	case received := <-done:
		c.Assert(received, Equals, false) // Should not receive frame
	case <-time.After(100 * time.Millisecond):
		// Timeout is expected - no frame should be sent
	}
}

func (s *CallbackMessagingSuite) TestSendNotConnected(c *C) {
	client, server := NewFakeConn()
	defer client.Close()
	defer server.Close()

	conn := NewCallbackConn(client)

	// Connection not established
	conn.setState(Disconnected)

	var callbackCalled bool
	var callbackErr error
	var wg sync.WaitGroup
	wg.Add(1)

	// Test send when not connected
	err := conn.Send("/queue/test", "text/plain", []byte("test message"), func(c *CallbackConn, dest string, err error) {
		callbackCalled = true
		callbackErr = err
		wg.Done()
	})

	c.Assert(err, Equals, ErrNotConnected)

	// Wait for callback
	wg.Wait()

	c.Assert(callbackCalled, Equals, true)
	c.Assert(callbackErr, Equals, ErrNotConnected)
}

func (s *CallbackMessagingSuite) TestSubscriptionEventString(c *C) {
	c.Assert(SubscriptionCreated.String(), Equals, "Created")
	c.Assert(SubscriptionActive.String(), Equals, "Active")
	c.Assert(SubscriptionUnsubscribed.String(), Equals, "Unsubscribed")
	c.Assert(SubscriptionError.String(), Equals, "Error")
	c.Assert(SubscriptionEvent(999).String(), Equals, "Unknown")
}

func (s *CallbackMessagingSuite) TestCallbackMessageShouldAck(c *C) {
	// Test with AckAuto - should not ack
	sub := &CallbackSubscription{ackMode: AckAuto}
	msg := &CallbackMessage{Subscription: sub}
	c.Assert(msg.ShouldAck(), Equals, false)

	// Test with AckClient - should ack
	sub = &CallbackSubscription{ackMode: AckClient}
	msg = &CallbackMessage{Subscription: sub}
	c.Assert(msg.ShouldAck(), Equals, true)

	// Test with nil subscription - should not ack
	msg = &CallbackMessage{Subscription: nil}
	c.Assert(msg.ShouldAck(), Equals, false)
}
