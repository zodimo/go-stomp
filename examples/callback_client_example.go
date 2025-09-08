package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/go-stomp/stomp/v3"
)

func main() {
	// Create a network connection
	conn, err := net.Dial("tcp", "localhost:61613")
	if err != nil {
		log.Fatal("Failed to connect to STOMP server:", err)
	}

	// Create callback-style STOMP connection
	callbackConn := stomp.NewCallbackConn(conn)

	// Set up callbacks for comprehensive monitoring
	setupCallbacks(callbackConn)

	// Connect to STOMP server
	fmt.Println("Connecting to STOMP server...")
	err = callbackConn.Connect(func(conn *stomp.CallbackConn, session, server string, version stomp.Version) {
		fmt.Printf("Connected! Session: %s, Server: %s, Version: %s\n", session, server, version)

		// Start example operations after successful connection
		go runExampleOperations(conn)
	})

	if err != nil {
		log.Fatal("Failed to connect:", err)
	}

	// Keep the program running for demonstration
	time.Sleep(30 * time.Second)

	// Graceful shutdown
	fmt.Println("Shutting down...")
	err = callbackConn.Close(func(conn *stomp.CallbackConn, err error) {
		if err != nil {
			fmt.Printf("Shutdown completed with error: %v\n", err)
		} else {
			fmt.Println("Shutdown completed successfully")
		}
	})

	if err != nil {
		log.Printf("Error during shutdown: %v", err)
	}

	// Wait a bit for shutdown to complete
	time.Sleep(2 * time.Second)
}

func setupCallbacks(conn *stomp.CallbackConn) {
	// Set up heart-beat monitoring
	conn.SetHeartBeatCallback(func(conn *stomp.CallbackConn, event stomp.HeartBeatEvent, err error) {
		if err != nil {
			fmt.Printf("Heart-beat error (%s): %v\n", event, err)
		} else {
			fmt.Printf("Heart-beat event: %s\n", event)
		}
	})

	// Set up health status monitoring
	conn.SetHealthStatusCallback(func(conn *stomp.CallbackConn, oldStatus, newStatus stomp.ConnectionHealth) {
		fmt.Printf("Health status changed: %s -> %s\n", oldStatus, newStatus)
	})

	// Set up error recovery handling
	conn.SetErrorRecoveryCallback(func(conn *stomp.CallbackConn, err error, recoveryAction stomp.RecoveryAction) stomp.RecoveryDecision {
		fmt.Printf("Error occurred: %v, suggested recovery: %s\n", err, recoveryAction)
		// For this example, always proceed with suggested recovery
		return stomp.DecisionProceed
	})

	// Set up connection state monitoring
	conn.SetStateChangeCallback(func(conn *stomp.CallbackConn, oldState, newState stomp.ConnectionState) {
		fmt.Printf("Connection state changed: %s -> %s\n", oldState, newState)
	})

	// Set up general error handling
	conn.SetErrorCallback(func(conn *stomp.CallbackConn, err error) {
		fmt.Printf("Connection error: %v\n", err)
	})
}

func runExampleOperations(conn *stomp.CallbackConn) {
	// Wait a moment for connection to stabilize
	time.Sleep(1 * time.Second)

	// Example 1: Basic messaging with callbacks
	fmt.Println("\n=== Basic Messaging Example ===")
	basicMessagingExample(conn)

	// Example 2: Transactional operations
	fmt.Println("\n=== Transactional Operations Example ===")
	transactionalExample(conn)

	// Example 3: Subscription and message handling
	fmt.Println("\n=== Subscription Example ===")
	subscriptionExample(conn)

	// Example 4: Connection statistics
	fmt.Println("\n=== Connection Statistics ===")
	statsExample(conn)
}

func basicMessagingExample(conn *stomp.CallbackConn) {
	// Send a simple message
	err := conn.Send("/queue/example", "text/plain", []byte("Hello, STOMP!"),
		func(conn *stomp.CallbackConn, destination string, err error) {
			if err != nil {
				fmt.Printf("Failed to send message to %s: %v\n", destination, err)
			} else {
				fmt.Printf("Message sent successfully to %s\n", destination)
			}
		})

	if err != nil {
		fmt.Printf("Error initiating send: %v\n", err)
	}
}

func transactionalExample(conn *stomp.CallbackConn) {
	// Start a transaction
	tx, err := conn.Begin(func(conn *stomp.CallbackConn, tx *stomp.CallbackTransaction, event stomp.TransactionEvent, err error) {
		if err != nil {
			fmt.Printf("Transaction error (%s): %v\n", event, err)
		} else {
			fmt.Printf("Transaction event: %s (ID: %s)\n", event, tx.Id())
		}
	})

	if err != nil {
		fmt.Printf("Failed to start transaction: %v\n", err)
		return
	}

	fmt.Printf("Started transaction: %s\n", tx.Id())

	// Send messages within the transaction
	err = tx.Send("/queue/tx-example", "application/json", []byte(`{"message": "transactional message 1"}`),
		func(conn *stomp.CallbackConn, destination string, err error) {
			if err != nil {
				fmt.Printf("Failed to send transactional message to %s: %v\n", destination, err)
			} else {
				fmt.Printf("Transactional message sent to %s\n", destination)
			}
		})

	if err != nil {
		fmt.Printf("Error sending transactional message: %v\n", err)
	}

	// Send another message within the transaction
	err = tx.Send("/queue/tx-example", "application/json", []byte(`{"message": "transactional message 2"}`),
		func(conn *stomp.CallbackConn, destination string, err error) {
			if err != nil {
				fmt.Printf("Failed to send second transactional message to %s: %v\n", destination, err)
			} else {
				fmt.Printf("Second transactional message sent to %s\n", destination)
			}
		})

	if err != nil {
		fmt.Printf("Error sending second transactional message: %v\n", err)
	}

	// Commit the transaction
	time.Sleep(100 * time.Millisecond) // Brief pause to let sends complete
	err = tx.Commit(func(conn *stomp.CallbackConn, tx *stomp.CallbackTransaction, event stomp.TransactionEvent, err error) {
		if err != nil {
			fmt.Printf("Transaction commit failed: %v\n", err)
		} else {
			fmt.Printf("Transaction committed successfully: %s\n", tx.Id())
		}
	})

	if err != nil {
		fmt.Printf("Error committing transaction: %v\n", err)
	}
}

func subscriptionExample(conn *stomp.CallbackConn) {
	// Subscribe to a queue with message handler
	subscription, err := conn.Subscribe("/queue/example", stomp.AckClient,
		func(conn *stomp.CallbackConn, message *stomp.CallbackMessage) {
			fmt.Printf("Received message: %s (from %s)\n", string(message.Body), message.Destination)

			// Acknowledge the message
			if message.ShouldAck() {
				err := conn.Ack(message, func(conn *stomp.CallbackConn, messageId string, err error) {
					if err != nil {
						fmt.Printf("Failed to acknowledge message %s: %v\n", messageId, err)
					} else {
						fmt.Printf("Message acknowledged: %s\n", messageId)
					}
				})
				if err != nil {
					fmt.Printf("Error initiating ack: %v\n", err)
				}
			}
		},
		func(conn *stomp.CallbackConn, subscription *stomp.CallbackSubscription, event stomp.SubscriptionEvent, err error) {
			if err != nil {
				fmt.Printf("Subscription error (%s): %v\n", event, err)
			} else {
				fmt.Printf("Subscription event: %s (ID: %s, Destination: %s)\n",
					event, subscription.Id(), subscription.Destination())
			}
		})

	if err != nil {
		fmt.Printf("Failed to subscribe: %v\n", err)
		return
	}

	fmt.Printf("Subscribed to %s with ID: %s\n", subscription.Destination(), subscription.Id())

	// Let subscription receive messages for a while
	time.Sleep(5 * time.Second)

	// Unsubscribe
	err = conn.Unsubscribe(subscription, func(conn *stomp.CallbackConn, subscription *stomp.CallbackSubscription, event stomp.SubscriptionEvent, err error) {
		if err != nil {
			fmt.Printf("Unsubscribe error: %v\n", err)
		} else {
			fmt.Printf("Successfully unsubscribed from %s\n", subscription.Destination())
		}
	})

	if err != nil {
		fmt.Printf("Error initiating unsubscribe: %v\n", err)
	}
}

func statsExample(conn *stomp.CallbackConn) {
	stats := conn.GetConnectionStats()
	health := conn.GetHealthStatus()

	fmt.Printf("Connection Health: %s\n", health)
	fmt.Printf("Frames Sent: %d\n", stats.FramesSent)
	fmt.Printf("Frames Received: %d\n", stats.FramesReceived)
	fmt.Printf("Heart-beats Sent: %d\n", stats.HeartBeatsSent)
	fmt.Printf("Heart-beats Received: %d\n", stats.HeartBeatsReceived)
	fmt.Printf("Connected At: %s\n", stats.ConnectedAt.Format(time.RFC3339))

	if !stats.LastHeartBeatSent.IsZero() {
		fmt.Printf("Last Heart-beat Sent: %s\n", stats.LastHeartBeatSent.Format(time.RFC3339))
	}
	if !stats.LastHeartBeatReceived.IsZero() {
		fmt.Printf("Last Heart-beat Received: %s\n", stats.LastHeartBeatReceived.Format(time.RFC3339))
	}
	if stats.LastError != nil {
		fmt.Printf("Last Error: %v\n", stats.LastError)
	}
}
