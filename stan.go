// Package stan provides a NATS Streaming broker
package stan // import "go.unistack.org/micro-broker-stan/v3"

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	stan "github.com/nats-io/stan.go"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/util/id"
)

type stanBroker struct {
	sync.RWMutex
	addrs          []string
	conn           stan.Conn
	opts           broker.Options
	sopts          stan.Options
	nopts          []stan.Option
	clusterID      string
	clientID       string
	connectTimeout time.Duration
	connectRetry   bool
	init           bool
	done           chan struct{}
}

type subscriber struct {
	t    string
	s    stan.Subscription
	dq   bool
	opts broker.SubscribeOptions
}

type publication struct {
	t   string
	msg *stan.Msg
	m   *broker.Message
	err error
}

func (n *publication) Topic() string {
	return n.t
}

func (n *publication) Message() *broker.Message {
	return n.m
}

func (n *publication) Ack() error {
	return n.msg.Ack()
}

func (n *publication) Error() error {
	return n.err
}

func (n *publication) SetError(err error) {
	n.err = err
}

func (n *subscriber) Options() broker.SubscribeOptions {
	return n.opts
}

func (n *subscriber) Topic() string {
	return n.t
}

func (n *subscriber) Unsubscribe(ctx context.Context) error {
	if n.s == nil {
		return nil
	}
	// go-micro server Unsubscribe can't handle durable queues, so close as stan suggested
	// from nats streaming readme:
	// When a client disconnects, the streaming server is not notified, hence the importance of calling Close()
	if !n.dq {
		err := n.s.Unsubscribe()
		if err != nil {
			return err
		}
	}
	return n.Close()
}

func (n *subscriber) Close() error {
	if n.s != nil {
		return n.s.Close()
	}
	return nil
}

func (n *stanBroker) Address() string {
	return strings.Join(n.addrs, ",")
}

func setAddrs(addrs []string) []string {
	cAddrs := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if len(addr) == 0 {
			continue
		}
		if !strings.HasPrefix(addr, "nats://") {
			addr = "nats://" + addr
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{stan.DefaultNatsURL}
	}
	return cAddrs
}

func (n *stanBroker) reconnectCB(c stan.Conn, err error) {
	if n.connectRetry {
		if err := n.connect(n.opts.Context); err != nil {
			if n.opts.Logger.V(logger.ErrorLevel) {
				n.opts.Logger.Errorf(n.opts.Context, "broker [stan] reconnect err: %v", err)
			}
		}
	}
}

func (n *stanBroker) connect(ctx context.Context) error {
	timeout := make(<-chan time.Time)

	n.RLock()
	if n.connectTimeout > 0 {
		timeout = time.After(n.connectTimeout)
	}
	clusterID := n.clusterID
	clientID := n.clientID
	nopts := n.nopts
	n.RUnlock()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	fn := func() error {
		c, err := stan.Connect(clusterID, clientID, nopts...)
		if err == nil {
			n.Lock()
			n.conn = c
			n.Unlock()
		}
		return err
	}

	// don't wait for first try
	if err := fn(); err == nil {
		return nil
	}

	n.RLock()
	done := n.done
	n.RUnlock()

	// wait loop
	for {
		select {
		// context closed
		case <-n.opts.Context.Done():
			return nil
		// call close, don't wait anymore
		case <-done:
			return nil
		//  in case of timeout fail with a timeout error
		case <-timeout:
			return fmt.Errorf("[stan]: timeout connect to %v", n.addrs)
		// got a tick, try to connect
		case <-ticker.C:
			err := fn()
			if err == nil && n.opts.Logger.V(logger.InfoLevel) {
				{
					n.opts.Logger.Infof(n.opts.Context, "[stan]: successeful connected to %v", n.addrs)
				}
				return nil
			}
			if n.opts.Logger.V(logger.ErrorLevel) {
				n.opts.Logger.Errorf(n.opts.Context, "[stan]: failed to connect %v: %v", n.addrs, err)
			}
		}
	}

	return nil
}

func (n *stanBroker) Connect(ctx context.Context) error {
	n.RLock()
	if n.conn != nil {
		n.RUnlock()
		return nil
	}
	n.RUnlock()

	clusterID, ok := n.opts.Context.Value(clusterIDKey{}).(string)
	if !ok || len(clusterID) == 0 {
		return fmt.Errorf("must specify ClusterID Option")
	}
	var err error
	clientID, ok := n.opts.Context.Value(clientIDKey{}).(string)
	if !ok || len(clientID) == 0 {
		clientID, err = id.New()
		if err != nil {
			return err
		}
	}

	n.Lock()
	if v, ok := n.opts.Context.Value(connectRetryKey{}).(bool); ok && v {
		n.connectRetry = true
	}

	if td, ok := n.opts.Context.Value(connectTimeoutKey{}).(time.Duration); ok {
		n.connectTimeout = td
	}

	if n.sopts.ConnectionLostCB != nil && n.connectRetry {
		n.Unlock()
		return fmt.Errorf("impossible to use custom ConnectionLostCB and ConnectRetry(true)")
	}

	nopts := []stan.Option{
		stan.NatsURL(n.sopts.NatsURL),
		stan.NatsConn(n.sopts.NatsConn),
		stan.ConnectWait(n.sopts.ConnectTimeout),
		stan.PubAckWait(n.sopts.AckTimeout),
		stan.MaxPubAcksInflight(n.sopts.MaxPubAcksInflight),
		stan.Pings(n.sopts.PingInterval, n.sopts.PingMaxOut),
	}

	if n.connectRetry {
		nopts = append(nopts, stan.SetConnectionLostHandler(n.reconnectCB))
	}

	nopts = append(nopts, stan.NatsURL(strings.Join(n.addrs, ",")))

	n.nopts = nopts
	n.clusterID = clusterID
	n.clientID = clientID
	n.Unlock()

	return n.connect(ctx)
}

func (n *stanBroker) Disconnect(ctx context.Context) error {
	var err error

	n.Lock()
	defer n.Unlock()

	if n.done != nil {
		close(n.done)
		n.done = nil
	}
	if n.conn != nil {
		err = n.conn.Close()
	}
	return err
}

func (n *stanBroker) Init(opts ...broker.Option) error {
	if len(opts) == 0 && n.init {
		return nil
	}

	if err := n.opts.Register.Init(); err != nil {
		return err
	}
	if err := n.opts.Tracer.Init(); err != nil {
		return err
	}
	if err := n.opts.Logger.Init(); err != nil {
		return err
	}
	if err := n.opts.Meter.Init(); err != nil {
		return err
	}

	for _, o := range opts {
		o(&n.opts)
	}
	n.addrs = setAddrs(n.opts.Addrs)

	n.init = true
	return nil
}

func (n *stanBroker) Options() broker.Options {
	return n.opts
}

func (n *stanBroker) BatchPublish(ctx context.Context, msg []*broker.Message, opts ...broker.PublishOption) error {
	var err error
	var buf []byte

	var wg sync.WaitGroup

	msgs := make(map[string][][]byte)

	options := broker.NewPublishOptions(opts...)
	wg.Add(len(msg))

	for _, m := range msg {
		if options.BodyOnly {
			buf = m.Body
		} else {
			buf, err = n.opts.Codec.Marshal(m)
			if err != nil {
				return err
			}
		}
		topic, _ := m.Header.Get(metadata.HeaderTopic)
		msgs[topic] = append(msgs[topic], buf)
	}

	var ackErr error

	ackHandler := func(ackedNuid string, err error) {
		wg.Done()
		if err != nil {
			ackErr = err
		}
	}

	n.RLock()
	defer n.RUnlock()

	for topic, ms := range msgs {
		for _, m := range ms {
			if _, err := n.conn.PublishAsync(topic, m, ackHandler); err != nil {
				return err
			}
		}
	}

	wg.Wait()

	return ackErr
}

func (n *stanBroker) Publish(ctx context.Context, topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	var buf []byte
	var err error

	options := broker.NewPublishOptions(opts...)

	if options.BodyOnly {
		buf = msg.Body
	} else {
		buf, err = n.opts.Codec.Marshal(msg)
		if err != nil {
			return err
		}
	}

	n.RLock()
	defer n.RUnlock()
	return n.conn.Publish(topic, buf)
}

func (n *stanBroker) BatchSubscribe(ctx context.Context, topic string, handler broker.BatchHandler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	return nil, nil
}

func (n *stanBroker) Subscribe(ctx context.Context, topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	n.RLock()
	if n.conn == nil {
		n.RUnlock()
		return nil, fmt.Errorf("not connected")
	}
	n.RUnlock()

	options := broker.NewSubscribeOptions(opts...)

	if subscribeContext, ok := options.Context.Value(subscribeContextKey{}).(context.Context); ok && subscribeContext != nil {
		ctx = subscribeContext
	}

	var stanOpts []stan.SubscriptionOption
	if options.AutoAck {
		stanOpts = append(stanOpts, stan.SetManualAckMode())
	}

	if subOpts, ok := options.Context.Value(subscribeOptionKey{}).([]stan.SubscriptionOption); ok && len(subOpts) > 0 {
		stanOpts = append(stanOpts, subOpts...)
	}

	bopts := stan.DefaultSubscriptionOptions
	for _, bopt := range stanOpts {
		if err := bopt(&bopts); err != nil {
			return nil, err
		}
	}

	options.AutoAck = !bopts.ManualAcks

	if dn, ok := n.opts.Context.Value(durableKey{}).(string); ok && len(dn) > 0 {
		stanOpts = append(stanOpts, stan.DurableName(dn))
		bopts.DurableName = dn
	}

	fn := func(msg *stan.Msg) {
		eh := n.opts.ErrorHandler

		if options.ErrorHandler != nil {
			eh = options.ErrorHandler
		}

		p := &publication{m: &broker.Message{}, msg: msg, t: msg.Subject}
		if options.BodyOnly {
			p.m.Body = msg.Data
		} else {
			// unmarshal message
			if err := n.opts.Codec.Unmarshal(msg.Data, p.m); err != nil {
				p.err = err
				p.m.Body = msg.Data
				eh(p)
				return
			}
		}
		// execute the handler
		p.err = handler(p)
		// if there's no error and success auto ack is enabled ack it
		if p.err == nil && options.AutoAck {
			msg.Ack()
		}
		if p.err != nil {
			eh(p)
		}
	}

	var sub stan.Subscription
	var err error

	n.RLock()
	if len(options.Group) > 0 {
		sub, err = n.conn.QueueSubscribe(topic, options.Group, fn, stanOpts...)
	} else {
		sub, err = n.conn.Subscribe(topic, fn, stanOpts...)
	}
	n.RUnlock()
	if err != nil {
		return nil, err
	}
	return &subscriber{dq: len(bopts.DurableName) > 0, s: sub, opts: options, t: topic}, nil
}

func (n *stanBroker) String() string {
	return "stan"
}

func (n *stanBroker) Name() string {
	return n.opts.Name
}

func NewBroker(opts ...broker.Option) broker.Broker {
	options := broker.NewOptions(opts...)

	stanOpts := stan.GetDefaultOptions()
	if n, ok := options.Context.Value(optionsKey{}).(stan.Options); ok {
		stanOpts = n
	}

	if len(options.Addrs) == 0 {
		options.Addrs = strings.Split(stanOpts.NatsURL, ",")
	}

	nb := &stanBroker{
		done:  make(chan struct{}),
		opts:  options,
		sopts: stanOpts,
		addrs: setAddrs(options.Addrs),
	}

	return nb
}
