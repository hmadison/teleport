// Copyright 2022 Gravitational, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"crypto/tls"
	"net"
	"sync"

	clientapi "github.com/gravitational/teleport/api/client/proto"

	"github.com/gravitational/teleport"
	"github.com/gravitational/teleport/api/metadata"
	"github.com/gravitational/teleport/api/types"
	"github.com/gravitational/teleport/lib/auth"
	"github.com/gravitational/teleport/lib/services"
	"github.com/gravitational/teleport/lib/utils"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

const (
	errorExistingTunnelNotFound = "EXISTING_TUNNEL_NOT_FOUND"
	errorExistingTunnelInvalid  = "EXISTING_TUNNEL_INVALID"
	errorNewTunnelDial          = "NEW_TUNNEL_DIAL"
	errorStreamStart            = "STREAM_START"
	errorTransientFailure       = "TRANSIENT_FAILURE"
	errorAuthClient             = "AUTH_CLIENT_ERROR"
	errorProxiesUnavailable     = "PROXIES_NOT_AVAILABLE"
)

// ClientConfig configures a Client instance.
type ClientConfig struct {
	// Context is a signalling context
	Context context.Context
	// ID is the ID of this server proxy
	ID string
	// AuthClient is an auth client
	AuthClient auth.ClientI
	// AccessPoint is a caching auth client
	AccessPoint auth.ProxyAccessPoint
	// TLSConfig is the proxy client TLS configuration.
	TLSConfig *tls.Config
	// Log is the proxy client logger.
	Log logrus.FieldLogger
	// Clock is used to control connection cleanup ticker.
	Clock clockwork.Clock

	// getConfigForServer updates the client tls config.
	// configurable for testing purposes.
	getConfigForServer func() (*tls.Config, error)

	// sync runs proxy and connection syncing operations
	// configurable for testing purposes
	sync func()
}

// checkAndSetDefaults checks and sets default values
func (c *ClientConfig) checkAndSetDefaults() error {
	if c.Log == nil {
		c.Log = logrus.New()
	}

	c.Log = c.Log.WithField(
		trace.Component,
		teleport.Component(teleport.ComponentProxyPeer),
	)

	if c.Clock == nil {
		c.Clock = clockwork.NewRealClock()
	}

	if c.Context == nil {
		c.Context = context.Background()
	}

	if c.ID == "" {
		return trace.BadParameter("missing parameter ID")
	}

	if c.AuthClient == nil {
		return trace.BadParameter("missing auth client")
	}

	if c.AccessPoint == nil {
		return trace.BadParameter("missing access cache")
	}

	if c.TLSConfig == nil {
		return trace.BadParameter("missing tls config")
	}

	if len(c.TLSConfig.Certificates) == 0 {
		return trace.BadParameter("missing tls certificate")
	}

	if c.getConfigForServer == nil {
		c.getConfigForServer = getConfigForServer(c.TLSConfig, c.AccessPoint, c.Log)
	}

	return nil
}

// clientConn hold info about a dialed grpc connection
type clientConn struct {
	*grpc.ClientConn
	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	id   string
	addr string
}

// Client is a peer proxy service client using grpc and tls.
type Client struct {
	sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc

	config  ClientConfig
	conns   map[string]*clientConn
	metrics *clientMetrics
}

type connections struct{}

// NewClient creats a new peer proxy client.
func NewClient(config ClientConfig) (*Client, error) {
	err := config.checkAndSetDefaults()
	if err != nil {
		return nil, trace.Wrap(err)
	}

	metrics, err := newClientMetrics()
	if err != nil {
		return nil, trace.Wrap(err)
	}

	closeContext, cancel := context.WithCancel(config.Context)

	c := &Client{
		config:  config,
		ctx:     closeContext,
		cancel:  cancel,
		conns:   make(map[string]*clientConn),
		metrics: metrics,
	}

	if c.config.sync != nil {
		go c.config.sync()
	} else {
		go c.sync()
	}

	return c, nil
}

// sync runs the proxy watcher functionality.
func (c *Client) sync() {
	proxyWatcher, err := services.NewProxyWatcher(c.ctx, services.ProxyWatcherConfig{
		ResourceWatcherConfig: services.ResourceWatcherConfig{
			Component: teleport.Component(teleport.ComponentProxyPeer),
			Client:    c.config.AccessPoint,
			Log:       c.config.Log,
		},
	})
	if err != nil {
		c.config.Log.Errorf("Error initializing proxy peer watcher: %+v.", err)
		return
	}
	defer proxyWatcher.Close()

	for {
		select {
		case <-c.ctx.Done():
			c.config.Log.Debug("Stopping peer proxy sync.")
			return
		case proxies := <-proxyWatcher.ProxiesC:
			if err := c.updateConnections(proxies); err != nil {
				c.config.Log.Errorf("Error syncing peer proxies: %+v.", err)
			}
		}
	}
}

func (c *Client) updateConnections(proxies []types.Server) error {
	c.RLock()
	var errs []error

	toDial := make(map[string]types.Server)
	for _, proxy := range proxies {
		toDial[proxy.GetName()] = proxy
	}

	toDelete := make([]string, 0)
	toKeep := make(map[string]*clientConn)
	for id, conn := range c.conns {
		proxy, ok := toDial[id]

		// delete nonexistent connections
		if !ok {
			toDelete = append(toDelete, id)
			continue
		}

		// peer address changed
		if conn.addr != proxy.GetPeerAddr() {
			toDelete = append(toDelete, id)
			continue
		}

		// test existing connections
		if err := c.testConnection(conn); err != nil {
			errs = append(errs, err)
			toDelete = append(toDelete, id)
			continue
		}

		toKeep[id] = conn
	}

	for id, proxy := range toDial {
		// skips itself
		if id == c.config.ID {
			continue
		}

		// skip existing connection. they've been tested above.
		if _, ok := toKeep[id]; ok {
			continue
		}

		// establish new connections
		conn, err := c.connect(id, proxy.GetPeerAddr())
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if err := c.testConnection(conn); err != nil {
			errs = append(errs, err)
			conn.Close()
			continue
		}

		toKeep[id] = conn
	}
	c.RUnlock()

	c.Lock()
	defer c.Unlock()

	for _, id := range toDelete {
		if conn, ok := c.conns[id]; ok {
			c.closeConn(conn)
		}
	}
	c.conns = toKeep

	return trace.NewAggregate(errs...)
}

// Dial dials a node through a peer proxy.
func (c *Client) Dial(
	proxyIDs []string,
	nodeID string,
	src net.Addr,
	dst net.Addr,
	tunnelType types.TunnelType,
) (net.Conn, error) {
	stream, _, err := c.dial(proxyIDs)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	// send dial request as the first frame
	if err = stream.Send(&clientapi.Frame{
		Message: &clientapi.Frame_DialRequest{
			DialRequest: &clientapi.DialRequest{
				NodeID:     nodeID,
				TunnelType: tunnelType,
				Source: &clientapi.NetAddr{
					Addr:    src.String(),
					Network: src.Network(),
				},
				Destination: &clientapi.NetAddr{
					Addr:    dst.String(),
					Network: dst.Network(),
				},
			},
		},
	}); err != nil {
		return nil, trace.Wrap(err)
	}

	conn := newStreamConn(stream, src, dst)
	go conn.start()

	return conn, nil
}

// Close closes all existing client connections.
func (c *Client) Close() error {
	c.Lock()
	defer c.Unlock()

	var errs []error
	for _, conn := range c.conns {
		if err := c.closeConn(conn); err != nil {
			errs = append(errs, err)
		}
	}
	c.cancel()
	return trace.NewAggregate(errs...)
}

// closeConn closes and removes a clientConn from cache.
// This function is not thread safe.
func (c *Client) closeConn(conn *clientConn) (err error) {
	conn.cancel()
	conn.wg.Wait() // wait for streams to gracefully end
	err = conn.Close()
	delete(c.conns, conn.id)
	return trace.Wrap(err)
}

// dial opens a new stream to one of the supplied proxy ids.
// it tries to find an existing grpc.ClientConn or initializes a new connection
// to one of the proxies otherwise.
// The boolean returned in the second argument is intended for testing purposes,
// to indicates whether the connection was cached or newly established.
func (c *Client) dial(proxyIDs []string) (clientapi.ProxyService_DialNodeClient, bool, error) {
	c.RLock()

	// try to dial existing connections.
	var stream clientapi.ProxyService_DialNodeClient
	errs := make([]error, 0)
	ids := make(map[string]struct{})
	for _, id := range proxyIDs {
		ids[id] = struct{}{}

		conn, ok := c.conns[id]
		if !ok {
			c.metrics.reportTunnelError(errorExistingTunnelNotFound)
			continue
		}

		var err error
		stream, err = c.startStream(conn)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		break
	}
	c.RUnlock()

	if stream != nil {
		return stream, true, nil
	}

	if len(errs) != 0 {
		c.metrics.reportTunnelError(errorExistingTunnelInvalid)
		return nil, false, trace.NewAggregate(errs...)
	}

	// proxy does not exist in cache.
	// get list of proxies directly from auth.
	c.Lock()
	defer c.Unlock()
	proxies, err := c.config.AuthClient.GetProxies()
	if err != nil {
		c.metrics.reportTunnelError(errorAuthClient)
		return nil, false, trace.NewAggregate(errs...)
	}

	errs = make([]error, 0)
	for _, proxy := range proxies {
		id := proxy.GetName()
		if _, ok := ids[id]; !ok {
			continue
		}

		conn, err := c.connect(id, proxy.GetPeerAddr())
		if err != nil {
			errs = append(errs, err)
			continue
		}

		stream, err := c.startStream(conn)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		c.conns[id] = conn
		return stream, false, nil
	}

	c.metrics.reportTunnelError(errorProxiesUnavailable)
	return nil, false, trace.NotFound("Error dialling all proxies: %+v", trace.NewAggregate(errs...).Error())
}

// connect dials a new connection to proxyAddr.
func (c *Client) connect(id string, proxyPeerAddr string) (*clientConn, error) {
	tlsConfig, err := c.config.getConfigForServer()
	if err != nil {
		c.metrics.reportTunnelError(errorNewTunnelDial)
		return nil, trace.Wrap(err, "Error updating client tls config")
	}

	connCtx, cancel := context.WithCancel(c.ctx)

	transportCreds := newProxyCredentials(credentials.NewTLS(tlsConfig))
	conn, err := grpc.DialContext(
		connCtx,
		proxyPeerAddr,
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithStatsHandler(newStatsHandler(c.metrics)),
		grpc.WithChainStreamInterceptor(metadata.StreamClientInterceptor, utils.GRPCClientStreamErrorInterceptor),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                peerKeepAlive,
			Timeout:             peerTimeout,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		c.metrics.reportTunnelError(errorNewTunnelDial)
		return nil, trace.Wrap(err, "Error dialling proxy %+v", id)
	}

	return &clientConn{
		ClientConn: conn,
		ctx:        connCtx,
		cancel:     cancel,
		wg:         new(sync.WaitGroup),
		id:         id,
		addr:       proxyPeerAddr,
	}, nil
}

// startStream opens a new stream to the provided connection.
func (c *Client) startStream(conn *clientConn) (clientapi.ProxyService_DialNodeClient, error) {
	client := clientapi.NewProxyServiceClient(conn.ClientConn)

	stream, err := client.DialNode(conn.ctx)
	if err != nil {
		c.metrics.reportTunnelError(errorStreamStart)
		return nil, trace.Wrap(err, "Error opening stream to proxy %+v", conn.id)
	}

	conn.wg.Add(1)
	go func() {
		<-conn.ctx.Done()
		stream.CloseSend()
		conn.wg.Done()
	}()

	return stream, nil
}

// testConnection opens a new stream to the provided connection and
// immediately closes it.
func (c *Client) testConnection(conn *clientConn) error {
	client := clientapi.NewProxyServiceClient(conn.ClientConn)

	stream, err := client.DialNode(conn.ctx)
	if err != nil {
		if conn.GetState() == connectivity.TransientFailure {
			c.metrics.reportTunnelError(errorTransientFailure)
			return nil
		}
		c.metrics.reportTunnelError(errorStreamStart)
		return trace.Wrap(err, "Error opening stream to proxy %+v", conn.id)
	}

	if err := stream.CloseSend(); err != nil {
		return trace.Wrap(err)
	}
	return nil
}
