package rocketmq

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport"

	"github.com/tx7do/kratos-transport/broker"
	"github.com/tx7do/kratos-transport/broker/rocketmq"
)

var (
	_ transport.Server     = (*Server)(nil)
	_ transport.Endpointer = (*Server)(nil)
)

type SubscriberMap map[string]broker.Subscriber

type SubscribeOption struct {
	handler broker.Handler
	binder  broker.Binder
	opts    []broker.SubscribeOption
}
type SubscribeOptionMap map[string]*SubscribeOption

type Server struct {
	broker.Broker
	brokerOpts []broker.Option

	subscribers    SubscriberMap
	subscriberOpts SubscribeOptionMap

	sync.RWMutex
	started bool

	log     *log.Helper
	baseCtx context.Context
	err     error
}

func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		baseCtx:        context.Background(),
		log:            log.NewHelper(log.GetLogger(), log.WithMessageKey("[rocketmq]")),
		subscribers:    SubscriberMap{},
		subscriberOpts: SubscribeOptionMap{},
		brokerOpts:     []broker.Option{},
		started:        false,
	}

	srv.init(opts...)

	srv.Broker = rocketmq.NewBroker(srv.brokerOpts...)

	return srv
}

func (s *Server) init(opts ...ServerOption) {
	for _, o := range opts {
		o(s)
	}
}

func (s *Server) Name() string {
	return "rocketmq"
}

func (s *Server) Start(ctx context.Context) error {
	if s.err != nil {
		return s.err
	}

	if s.started {
		return nil
	}

	_ = s.Init()

	s.err = s.Connect()
	if s.err != nil {
		return s.err
	}

	s.log.Infof("[rocketmq] server listening on: %s", s.Address())

	s.err = s.doRegisterSubscriberMap()
	if s.err != nil {
		return s.err
	}

	s.baseCtx = ctx
	s.started = true

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	for _, sub := range s.subscribers {
		_ = sub.Unsubscribe()
	}
	s.log.Info("[rocketmq] server stopping")
	s.started = false
	return s.Disconnect()
}

func (s *Server) Endpoint() (*url.URL, error) {
	if s.err != nil {
		return nil, s.err
	}

	addr := s.Address()
	if !strings.HasPrefix(addr, "tcp://") {
		addr = "tcp://" + addr
	}

	return url.Parse(addr)
}

func (s *Server) RegisterSubscriber(ctx context.Context, topic, groupName string, handler broker.Handler, binder broker.Binder, opts ...broker.SubscribeOption) error {
	s.Lock()
	defer s.Unlock()

	if s.baseCtx == nil {
		s.baseCtx = context.Background()
	}
	if ctx == nil {
		ctx = s.baseCtx
	}
	if topic == "" {
		return errors.New("topic is empty")
	}
	if groupName == "" {
		return errors.New("group is empty")
	}

	opts = append(opts, broker.WithQueueName(groupName))

	// context必须要插入到头部，否则后续传入的配置会被覆盖掉。
	opts = append([]broker.SubscribeOption{broker.WithSubscribeContext(ctx)}, opts...)

	if s.started {
		return s.doRegisterSubscriber(topic, handler, binder, opts...)
	} else {
		var options broker.SubscribeOptions
		for _, opt := range opts {
			opt(&options)
		}
		s.subscriberOpts[s.subscriberKey(groupName, topic, options.MessageTag)] = &SubscribeOption{handler: handler, binder: binder, opts: opts}
	}
	return nil
}

func (s *Server) doRegisterSubscriber(topic string, handler broker.Handler, binder broker.Binder, opts ...broker.SubscribeOption) error {
	sub, err := s.Subscribe(topic, handler, binder, opts...)
	if err != nil {
		return err
	}

	s.subscribers[s.subscriberKey(topic, sub.Options().Queue, sub.Options().MessageTag)] = sub

	return nil
}

func (s *Server) doRegisterSubscriberMap() error {
	for subscriberKey, opt := range s.subscriberOpts {
		_, topic, _ := s.parseSubscriberKey(subscriberKey)
		_ = s.doRegisterSubscriber(topic, opt.handler, opt.binder, opt.opts...)
	}
	s.subscriberOpts = SubscribeOptionMap{}
	return nil
}

func (s *Server) subscriberKey(groupName, topic, messageTag string) string {
	return fmt.Sprintf("%s:%s:%s", groupName, topic, messageTag)
}

func (s *Server) parseSubscriberKey(subscriberKey string) (groupName, topic, messageTag string) {
	keys := strings.Split(subscriberKey, ":")
	return keys[0], keys[1], keys[2]
}
