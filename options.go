package stan

import (
	"context"

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

// AckOnSuccess allow to AutoAck messages when handler returns without error
func AckOnSuccess() broker.SubscribeOption {
	return setSubscribeOption(ackSuccessKey{}, true)
}
