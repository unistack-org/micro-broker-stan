package stan

import (
	"context"
	"time"

	"github.com/micro/go-micro/broker"
	stan "github.com/nats-io/go-nats-streaming"
)

type optionsKey struct{}

// Options accepts stan.Options
func Options(opts stan.Options) broker.Option {
	return setBrokerOption(optionsKey{}, opts)
}

type clusterIDKey struct{}

// ClusterID specify cluster id to connect
func ClusterID(clusterID string) broker.Option {
	return setBrokerOption(clusterIDKey{}, clusterID)
}

type subscribeOptionKey struct{}

func SubscribeOption(opts ...stan.SubscriptionOption) broker.SubscribeOption {
	return setSubscribeOption(subscribeOptionKey{}, opts)
}

type subscribeContextKey struct{}

// SubscribeContext set the context for broker.SubscribeOption
func SubscribeContext(ctx context.Context) broker.SubscribeOption {
	return setSubscribeOption(subscribeContextKey{}, ctx)
}

type ackSuccessKey struct{}

// AckOnSuccess will automatically acknowledge messages when no error is returned
func AckOnSuccess() broker.SubscribeOption {
	return setSubscribeOption(ackSuccessKey{}, true)
}

type timeoutKey struct{}

// Timeout for connecting to broker -1 infinitive or time.Duration value
func Timeout(td time.Duration) broker.Option {
	return setBrokerOption(timeoutKey{}, td)
}

type reconnectKey struct{}

// Reconnect to broker in case of errors
func Reconnect(v bool) broker.Option {
	return setBrokerOption(reconnectKey{}, v)
}
