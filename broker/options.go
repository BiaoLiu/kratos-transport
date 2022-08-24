package broker

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/go-kratos/kratos/v2/encoding"
	"github.com/go-kratos/kratos/v2/log"
)

var DefaultCodec encoding.Codec = nil

type Options struct {
	Addrs []string

	Codec encoding.Codec

	ErrorHandler Handler

	Secure    bool
	TLSConfig *tls.Config

	Context context.Context

	Logger *log.Helper
}

type Option func(*Options)

func (o *Options) Apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func NewOptions() Options {
	opt := Options{
		Addrs: []string{},
		Codec: DefaultCodec,

		ErrorHandler: nil,

		Secure:    false,
		TLSConfig: nil,

		Context: context.Background(),

		Logger: log.NewHelper(log.GetLogger()),
	}

	return opt
}

func NewOptionsAndApply(opts ...Option) Options {
	opt := NewOptions()
	opt.Apply(opts...)
	return opt
}

func WithOptionContext(ctx context.Context) Option {
	return func(o *Options) {
		if o.Context == nil {
			o.Context = ctx
		}
	}
}

func OptionContextWithValue(k, v interface{}) Option {
	return func(o *Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, k, v)
	}
}

func WithAddress(addressList ...string) Option {
	return func(o *Options) {
		o.Addrs = addressList
	}
}

func WithCodec(codec encoding.Codec) Option {
	return func(o *Options) {
		o.Codec = codec
	}
}

func WithErrorHandler(handler Handler) Option {
	return func(o *Options) {
		o.ErrorHandler = handler
	}
}

func WithEnableSecure(enable bool) Option {
	return func(o *Options) {
		o.Secure = enable
	}
}

func WithTLSConfig(config *tls.Config) Option {
	return func(o *Options) {
		o.TLSConfig = config
	}
}

func WithLogger(logger *log.Helper) Option {
	return func(o *Options) {
		o.Logger = logger
	}
}

///////////////////////////////////////////////////////////////////////////////

type PublishOptions struct {
	Context context.Context
}

type PublishOption func(*PublishOptions)

func (o *PublishOptions) Apply(opts ...PublishOption) {
	for _, opt := range opts {
		opt(o)
	}
}

func NewPublishOptions(opts ...PublishOption) PublishOptions {
	opt := PublishOptions{
		Context: context.Background(),
	}

	opt.Apply(opts...)

	return opt
}

func PublishContextWithValue(k, v interface{}) PublishOption {
	return func(o *PublishOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, k, v)
	}
}

func WithPublishContext(ctx context.Context) PublishOption {
	return func(o *PublishOptions) {
		o.Context = ctx
	}
}

///////////////////////////////////////////////////////////////////////////////

type ConsumeRetry struct {
	MaxRetryCount  int64
	MaxRetryTime   time.Duration
	MinDelay       time.Duration
	Factor         float64
	HandleRetryEnd func(context.Context, Event)
}

type SubscribeOptions struct {
	AutoAck       bool
	Queue         string
	Context       context.Context
	MessageTag    string
	EnableTrace   bool
	NumOfMessages int
	ConsumeRetry  *ConsumeRetry
}

type SubscribeOption func(*SubscribeOptions)

func (o *SubscribeOptions) Apply(opts ...SubscribeOption) {
	for _, opt := range opts {
		opt(o)
	}
}

func NewSubscribeOptions(opts ...SubscribeOption) SubscribeOptions {
	opt := SubscribeOptions{
		AutoAck: true,
		Queue:   "",
		Context: context.Background(),
	}

	opt.Apply(opts...)

	return opt
}

func SubscribeContextWithValue(k, v interface{}) SubscribeOption {
	return func(o *SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, k, v)
	}
}

func EnableTrace() SubscribeOption {
	return func(o *SubscribeOptions) {
		o.EnableTrace = true
	}
}

func DisableAutoAck() SubscribeOption {
	return func(o *SubscribeOptions) {
		o.AutoAck = false
	}
}

func WithQueueName(name string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Queue = name
	}
}

func WithSubscribeContext(ctx context.Context) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Context = ctx
	}
}

func WithMessageTag(messageTag string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.MessageTag = messageTag
	}
}

func WithNumOfMessages(numOfMessages int) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.NumOfMessages = numOfMessages
	}
}

func WithConsumeRetry(maxRetryCount int64, maxRetryTime, minDelay time.Duration,
	factor float64, handleRetryEnd func(context.Context, Event)) SubscribeOption {
	return func(o *SubscribeOptions) {
		if maxRetryCount > 0 || maxRetryTime > 0 {
			if minDelay <= 0 {
				minDelay = 1 * time.Second
			}
			if factor <= 0 {
				factor = 1
			}
			o.ConsumeRetry = &ConsumeRetry{
				MaxRetryCount:  maxRetryCount,
				MaxRetryTime:   maxRetryTime,
				MinDelay:       minDelay,
				Factor:         factor,
				HandleRetryEnd: handleRetryEnd,
			}
		}
	}
}
