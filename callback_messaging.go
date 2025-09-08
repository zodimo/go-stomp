package stomp

import (
	"context"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
)

// Send sends a message to the specified destination with callback notification
func (c *CallbackConn) Send(destination, contentType string, body []byte, callback SendCallback, opts ...func(*frame.Frame) error) error {
	if c.GetState() != Connected {
		if callback != nil {
			callback(c, destination, ErrNotConnected)
		}
		return ErrNotConnected
	}

	// Create SEND frame using existing logic
	f, err := createSendFrame(destination, contentType, body, opts)
	if err != nil {
		if callback != nil {
			callback(c, destination, err)
		}
		return err
	}

	// Store callback for later use
	c.mu.Lock()
	c.sendCallback = callback
	c.mu.Unlock()

	// Send the frame asynchronously
	go c.performSend(f, destination)

	return nil
}

// SendWithReceipt sends a message with receipt confirmation and callback notification
func (c *CallbackConn) SendWithReceipt(destination, contentType string, body []byte, callback SendCallback, timeout time.Duration, opts ...func(*frame.Frame) error) error {
	if c.GetState() != Connected {
		if callback != nil {
			callback(c, destination, ErrNotConnected)
		}
		return ErrNotConnected
	}

	// Add receipt option to the frame options
	receiptId := allocateId()
	receiptOpt := func(f *frame.Frame) error {
		f.Header.Set(frame.Receipt, receiptId)
		return nil
	}

	// Prepend receipt option to existing options
	allOpts := append([]func(*frame.Frame) error{receiptOpt}, opts...)

	// Create SEND frame with receipt
	f, err := createSendFrame(destination, contentType, body, allOpts)
	if err != nil {
		if callback != nil {
			callback(c, destination, err)
		}
		return err
	}

	// Store callback for later use
	c.mu.Lock()
	c.sendCallback = callback
	c.mu.Unlock()

	// Send the frame with receipt handling
	go c.performSendWithReceipt(f, destination, receiptId, timeout)

	return nil
}

// performSend handles the actual sending of a message without receipt
func (c *CallbackConn) performSend(f *frame.Frame, destination string) {
	// Create frame writer using the standard io adapter
	writer := frame.NewWriter(c.ioAdapter)

	// Send SEND frame
	err := writer.Write(f)

	// Get callback and call it
	c.mu.RLock()
	callback := c.sendCallback
	c.mu.RUnlock()

	if callback != nil {
		callback(c, destination, err)
	}
}

// performSendWithReceipt handles sending with receipt confirmation
func (c *CallbackConn) performSendWithReceipt(f *frame.Frame, destination, receiptId string, timeout time.Duration) {
	// Create frame writer and reader using the standard io adapter
	writer := frame.NewWriter(c.ioAdapter)
	reader := frame.NewReader(c.ioAdapter)

	// Send SEND frame
	err := writer.Write(f)
	if err != nil {
		c.notifySendCallback(destination, err)
		return
	}

	// Wait for RECEIPT response with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
		// Receipt received, send successful
		c.notifySendCallback(destination, nil)
	case err := <-errorCh:
		// Error occurred
		c.notifySendCallback(destination, err)
	case <-ctx.Done():
		// Timeout occurred
		c.notifySendCallback(destination, ErrSendReceiptTimeout)
	}
}

// notifySendCallback calls the send callback if set
func (c *CallbackConn) notifySendCallback(destination string, err error) {
	c.mu.RLock()
	callback := c.sendCallback
	c.mu.RUnlock()

	if callback != nil {
		callback(c, destination, err)
	}
}

// Subscribe creates a subscription with message handler and callback notification
func (c *CallbackConn) Subscribe(destination string, ackMode AckMode, handler MessageHandler, callback SubscriptionCallback, opts ...func(*frame.Frame) error) (*CallbackSubscription, error) {
	if c.GetState() != Connected {
		if callback != nil {
			callback(c, nil, SubscriptionError, ErrNotConnected)
		}
		return nil, ErrNotConnected
	}

	// Generate subscription ID
	id := allocateId()

	// Create subscription
	subscription := &CallbackSubscription{
		id:          id,
		destination: destination,
		ackMode:     ackMode,
		handler:     handler,
		conn:        c,
		active:      false,
	}

	// Store subscription and handler
	c.mu.Lock()
	c.subscriptions[id] = subscription
	c.messageHandlers[id] = handler
	c.subscriptionCallback = callback
	c.mu.Unlock()

	// Create SUBSCRIBE frame
	subscribeFrame := frame.New(frame.SUBSCRIBE,
		frame.Destination, destination,
		frame.Id, id,
		frame.Ack, ackMode.String())

	// Apply options
	for _, opt := range opts {
		if opt != nil {
			if err := opt(subscribeFrame); err != nil {
				// Remove subscription on error
				c.mu.Lock()
				delete(c.subscriptions, id)
				delete(c.messageHandlers, id)
				c.mu.Unlock()

				if callback != nil {
					callback(c, subscription, SubscriptionError, err)
				}
				return nil, err
			}
		}
	}

	// Send subscribe frame asynchronously
	go c.performSubscribe(subscribeFrame, subscription)

	return subscription, nil
}

// performSubscribe handles the actual subscription process
func (c *CallbackConn) performSubscribe(subscribeFrame *frame.Frame, subscription *CallbackSubscription) {
	// Create frame writer using the standard io adapter
	writer := frame.NewWriter(c.ioAdapter)

	// Send SUBSCRIBE frame
	err := writer.Write(subscribeFrame)

	// Get callback
	c.mu.RLock()
	callback := c.subscriptionCallback
	c.mu.RUnlock()

	if err != nil {
		// Remove subscription on error
		c.mu.Lock()
		delete(c.subscriptions, subscription.id)
		delete(c.messageHandlers, subscription.id)
		c.mu.Unlock()

		if callback != nil {
			callback(c, subscription, SubscriptionError, err)
		}
		return
	}

	// Mark subscription as active
	subscription.active = true

	if callback != nil {
		callback(c, subscription, SubscriptionCreated, nil)
		callback(c, subscription, SubscriptionActive, nil)
	}
}

// Unsubscribe removes a subscription with callback notification
func (c *CallbackConn) Unsubscribe(subscription *CallbackSubscription, callback SubscriptionCallback) error {
	if c.GetState() != Connected {
		if callback != nil {
			callback(c, subscription, SubscriptionError, ErrNotConnected)
		}
		return ErrNotConnected
	}

	if subscription == nil {
		if callback != nil {
			callback(c, subscription, SubscriptionError, ErrInvalidSubscription)
		}
		return ErrInvalidSubscription
	}

	// Store callback
	c.mu.Lock()
	c.subscriptionCallback = callback
	c.mu.Unlock()

	// Create UNSUBSCRIBE frame with receipt
	receiptId := allocateId()
	unsubscribeFrame := frame.New(frame.UNSUBSCRIBE,
		frame.Id, subscription.id,
		frame.Receipt, receiptId)

	// Send unsubscribe frame asynchronously
	go c.performUnsubscribe(unsubscribeFrame, subscription, receiptId)

	return nil
}

// performUnsubscribe handles the actual unsubscription process
func (c *CallbackConn) performUnsubscribe(unsubscribeFrame *frame.Frame, subscription *CallbackSubscription, receiptId string) {
	// Create frame writer and reader using the standard io adapter
	writer := frame.NewWriter(c.ioAdapter)
	reader := frame.NewReader(c.ioAdapter)

	// Send UNSUBSCRIBE frame
	err := writer.Write(unsubscribeFrame)
	if err != nil {
		c.notifySubscriptionCallback(subscription, SubscriptionError, err)
		return
	}

	// Wait for RECEIPT response with timeout
	ctx, cancel := context.WithTimeout(context.Background(), c.unsubscribeReceiptTimeout)
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
		// Receipt received, unsubscribe successful
		c.cleanupSubscription(subscription)
		c.notifySubscriptionCallback(subscription, SubscriptionUnsubscribed, nil)
	case err := <-errorCh:
		// Error occurred
		c.notifySubscriptionCallback(subscription, SubscriptionError, err)
	case <-ctx.Done():
		// Timeout occurred
		c.notifySubscriptionCallback(subscription, SubscriptionError, ErrUnsubscribeReceiptTimeout)
	}
}

// cleanupSubscription removes subscription from internal maps
func (c *CallbackConn) cleanupSubscription(subscription *CallbackSubscription) {
	c.mu.Lock()
	defer c.mu.Unlock()

	subscription.active = false
	delete(c.subscriptions, subscription.id)
	delete(c.messageHandlers, subscription.id)
}

// notifySubscriptionCallback calls the subscription callback if set
func (c *CallbackConn) notifySubscriptionCallback(subscription *CallbackSubscription, event SubscriptionEvent, err error) {
	c.mu.RLock()
	callback := c.subscriptionCallback
	c.mu.RUnlock()

	if callback != nil {
		callback(c, subscription, event, err)
	}
}

// Ack acknowledges a message with callback notification
func (c *CallbackConn) Ack(message *CallbackMessage, callback AckCallback) error {
	if c.GetState() != Connected {
		if callback != nil {
			callback(c, message.ackId, ErrNotConnected)
		}
		return ErrNotConnected
	}

	// Store callback
	c.mu.Lock()
	c.ackCallback = callback
	c.mu.Unlock()

	// Create ACK frame using existing logic
	f, err := c.createCallbackAckNackFrame(message, true)
	if err != nil {
		if callback != nil {
			callback(c, message.ackId, err)
		}
		return err
	}

	if f != nil {
		// Send ACK frame asynchronously
		go c.performAck(f, message.ackId)
	} else {
		// No frame needed (e.g., AckAuto mode)
		if callback != nil {
			callback(c, message.ackId, nil)
		}
	}

	return nil
}

// Nack negatively acknowledges a message with callback notification
func (c *CallbackConn) Nack(message *CallbackMessage, callback AckCallback) error {
	if c.GetState() != Connected {
		if callback != nil {
			callback(c, message.ackId, ErrNotConnected)
		}
		return ErrNotConnected
	}

	// Store callback
	c.mu.Lock()
	c.ackCallback = callback
	c.mu.Unlock()

	// Create NACK frame using existing logic
	f, err := c.createCallbackAckNackFrame(message, false)
	if err != nil {
		if callback != nil {
			callback(c, message.ackId, err)
		}
		return err
	}

	if f != nil {
		// Send NACK frame asynchronously
		go c.performAck(f, message.ackId)
	} else {
		// No frame needed
		if callback != nil {
			callback(c, message.ackId, nil)
		}
	}

	return nil
}

// performAck handles the actual acknowledgment process
func (c *CallbackConn) performAck(f *frame.Frame, messageId string) {
	// Create frame writer using the standard io adapter
	writer := frame.NewWriter(c.ioAdapter)

	// Send ACK/NACK frame
	err := writer.Write(f)

	// Get callback and call it
	c.mu.RLock()
	callback := c.ackCallback
	c.mu.RUnlock()

	if callback != nil {
		callback(c, messageId, err)
	}
}

// createCallbackAckNackFrame creates ACK/NACK frame for callback messages
func (c *CallbackConn) createCallbackAckNackFrame(msg *CallbackMessage, ack bool) (*frame.Frame, error) {
	if !ack && !c.version.SupportsNack() {
		return nil, ErrNackNotSupported
	}

	if msg.Header == nil || msg.Subscription == nil || msg.Conn == nil {
		return nil, ErrNotReceivedMessage
	}

	if msg.Subscription.AckMode() == AckAuto {
		if ack {
			// not much point sending an ACK to an auto subscription
			return nil, nil
		} else {
			// sending a NACK for an ack:auto subscription makes no
			// sense
			return nil, ErrCannotNackAutoSub
		}
	}

	var f *frame.Frame
	if ack {
		f = frame.New(frame.ACK)
	} else {
		f = frame.New(frame.NACK)
	}

	switch msg.Subscription.AckMode() {
	case AckClient:
		if messageId, ok := msg.Header.Contains(frame.MessageId); ok {
			f.Header.Set(frame.MessageId, messageId)
		}
	case AckClientIndividual:
		if messageId, ok := msg.Header.Contains(frame.MessageId); ok {
			f.Header.Set(frame.MessageId, messageId)
		}
	}

	// Set subscription header for STOMP 1.1 and later
	if c.version != V10 {
		f.Header.Set(frame.Subscription, msg.Subscription.Id())
	}

	return f, nil
}
