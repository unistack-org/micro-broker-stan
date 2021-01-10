package stan

import (
	"context"
	"time"

	stan "github.com/nats-io/stan.go"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/server"
)

type optionsKey struct{}

// Options accepts stan.Options
func Options(opts stan.Options) broker.Option {
	return broker.SetOption(optionsKey{}, opts)
}

type clusterIDKey struct{}

// ClusterID specify cluster id to connect
func ClusterID(clusterID string) broker.Option {
	return broker.SetOption(clusterIDKey{}, clusterID)
}

type clientIDKey struct{}

// ClientID specify client id to connect
func ClientID(clientID string) broker.Option {
	return broker.SetOption(clientIDKey{}, clientID)
}

type subscribeOptionKey struct{}

func SubscribeOption(opts ...stan.SubscriptionOption) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeOptionKey{}, opts)
}

func ServerSubscriberOption(opts ...stan.SubscriptionOption) server.SubscriberOption {
	return server.SetSubscriberOption(subscribeOptionKey{}, opts)
}

type subscribeContextKey struct{}

// SubscribeContext set the context for broker.SubscribeOption
func SubscribeContext(ctx context.Context) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeContextKey{}, ctx)
}

type connectTimeoutKey struct{}

// ConnectTimeout timeout for connecting to broker -1 infinitive or time.Duration value
func ConnectTimeout(td time.Duration) broker.Option {
	return broker.SetOption(connectTimeoutKey{}, td)
}

type connectRetryKey struct{}

// ConnectRetry reconnect to broker in case of errors
func ConnectRetry(v bool) broker.Option {
	return broker.SetOption(connectRetryKey{}, v)
}

type durableKey struct{}

// DurableName sets the DurableName for the subscriber
func DurableName(name string) broker.Option {
	return broker.SetOption(durableKey{}, name)
}
