package gorethink

import (
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"net"
	"sabey.co/lagoon"
	"sync"
	"time"
)

var (
	errPoolClosed = errors.New("gorethink: pool is closed")
)

// A Pool is used to store a pool of connections to a single RethinkDB server
type Pool struct {
	host Host
	opts *ConnectOpts

	pool *lagoon.Lagoon

	mu     sync.RWMutex // protects following fields
	closed bool
}

// NewPool creates a new connection pool for the given host
func NewPool(host Host, opts *ConnectOpts) (*Pool, error) {
	initialCap := opts.InitialCap
	if initialCap <= 0 {
		// Fallback to MaxIdle if InitialCap is zero, this should be removed
		// when MaxIdle is removed
		initialCap = opts.MaxIdle
	}

	l_config := &lagoon.Config{
		Dial: func() (net.Conn, error) {
			conn, err := NewConnection(host.String(), opts)
			if err != nil {
				return nil, err
			}

			return conn, err
		},
		DialInitial: initialCap,
		IdleTimeout: opts.LagoonIdleTimeout,
		TickEvery:   opts.LagoonTickEvery,
		Buffer:      opts.LagoonBuffer,
	}
	if l_config.IdleTimeout < 1 {
		l_config.IdleTimeout = time.Minute * 3
	}
	if l_config.TickEvery < 1 {
		l_config.TickEvery = time.Second * 15
	}
	if l_config.Buffer == nil {
		// create a new buffer
		maxOpen := opts.MaxOpen
		if maxOpen <= 0 {
			maxOpen = 2
		}
		timeout := opts.LagoonTimeout
		if timeout <= 0 {
			timeout = time.Second * 30
		}
		l_config.Buffer = lagoon.CreateBuffer(maxOpen, timeout)
	}

	l, err := lagoon.CreateLagoon(l_config)
	if err != nil {
		return nil, err
	}

	return &Pool{
		pool: l,
		host: host,
		opts: opts,
	}, nil
}

// Ping verifies a connection to the database is still alive,
// establishing a connection if necessary.
func (p *Pool) Ping() error {
	_, pc, err := p.conn()
	if err != nil {
		return err
	}
	return pc.Close()
}

// Close closes the database, releasing any open resources.
//
// It is rare to Close a Pool, as the Pool handle is meant to be
// long-lived and shared between many goroutines.
func (p *Pool) Close() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil
	}

	p.pool.Close()

	return nil
}

func (p *Pool) conn() (*Connection, *lagoon.Connection, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, nil, errPoolClosed
	}

	nc, err := p.pool.Dial()
	if err != nil {
		return nil, nil, err
	}

	pc, ok := nc.(*lagoon.Connection)
	if !ok {
		// This should never happen!
		return nil, nil, fmt.Errorf("Invalid connection in pool")
	}

	conn, ok := pc.Conn.(*Connection)
	if !ok {
		// This should never happen!
		return nil, nil, fmt.Errorf("Invalid connection in pool")
	}

	return conn, pc, nil
}

// SetInitialPoolCap sets the initial capacity of the connection pool.
//
// Deprecated: This value should only be set when connecting
func (p *Pool) SetInitialPoolCap(n int) {
	return
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
//
// Deprecated: This value should only be set when connecting
func (p *Pool) SetMaxIdleConns(n int) {
	return
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
//
// Deprecated: This value should only be set when connecting
func (p *Pool) SetMaxOpenConns(n int) {
	return
}

// Query execution functions

// Exec executes a query without waiting for any response.
func (p *Pool) Exec(ctx context.Context, q Query) error {
	c, pc, err := p.conn()
	if err != nil {
		return err
	}
	defer pc.Close()

	_, _, err = c.Query(ctx, q)

	if c.isBad() {
		pc.Disable()
	}

	return err
}

// Query executes a query and waits for the response
func (p *Pool) Query(ctx context.Context, q Query) (*Cursor, error) {
	c, pc, err := p.conn()
	if err != nil {
		return nil, err
	}

	_, cursor, err := c.Query(ctx, q)

	if err == nil {
		cursor.releaseConn = releaseConn(c, pc)
	} else if c.isBad() {
		pc.Disable()
	}

	return cursor, err
}

// Server returns the server name and server UUID being used by a connection.
func (p *Pool) Server() (ServerResponse, error) {
	var response ServerResponse

	c, pc, err := p.conn()
	if err != nil {
		return response, err
	}
	defer pc.Close()

	response, err = c.Server()

	if c.isBad() {
		pc.Disable()
	}

	return response, err
}

func releaseConn(c *Connection, pc *lagoon.Connection) func() error {
	return func() error {
		if c.isBad() {
			pc.Disable()
		}

		return pc.Close()
	}
}
