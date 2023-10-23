package natsjson

import (
	"errors"
	"fmt"
	"os"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsclient "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func NewInProcessNATSServer() (conn *natsclient.Conn, js jetstream.JetStream, cleanup func(), err error) {
	tmp, err := os.MkdirTemp("", "nats_test")
	if err != nil {
		err = fmt.Errorf("failed to create temp directory for NATS storage: %w", err)
		return
	}
	server, err := natsserver.NewServer(&natsserver.Options{
		DontListen: true, // Don't make a TCP socket.
		JetStream:  true,
		StoreDir:   tmp,
	})
	if err != nil {
		err = fmt.Errorf("failed to create NATS server: %w", err)
		return
	}
	// Add logs to stdout.
	// server.ConfigureLogger()
	server.Start()
	cleanup = func() {
		server.Shutdown()
		os.RemoveAll(tmp)
	}

	if !server.ReadyForConnections(time.Second * 5) {
		err = errors.New("failed to start server after 5 seconds")
		return
	}

	// Create a connection.
	conn, err = natsclient.Connect("", natsclient.InProcessServer(server))
	if err != nil {
		err = fmt.Errorf("failed to connect to server: %w", err)
		return
	}

	// Create a JetStream client.
	js, err = jetstream.New(conn)
	if err != nil {
		err = fmt.Errorf("failed to create jetstream: %w", err)
		return
	}

	return
}
