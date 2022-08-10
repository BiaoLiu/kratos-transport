package rocketmq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	aliyun "github.com/aliyunmq/mq-http-go-sdk"
	"github.com/go-kratos/kratos/v2/log"
	gerr "github.com/gogap/errors"
	"github.com/panjf2000/ants/v2"
	"github.com/rfyiamcool/backoff"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/trace"

	"github.com/tx7do/kratos-transport/broker"
)

const (
	FirstRetryTime = "first_retry_time"
	RetriedCount   = "retried_count"
)

type aliyunBroker struct {
	nameServers   []string
	nameServerUrl string

	accessKey     string
	secretKey     string
	securityToken string

	instanceName string
	groupName    string
	retryCount   int
	namespace    string

	log *log.Helper

	connected bool
	sync.RWMutex
	opts broker.Options

	client    aliyun.MQClient
	producers map[string]aliyun.MQProducer
}

func newAliyunHttpBroker(options broker.Options) broker.Broker {
	return &aliyunBroker{
		producers:  make(map[string]aliyun.MQProducer),
		opts:       options,
		log:        log.NewHelper(log.GetLogger()),
		retryCount: 2,
	}
}

func (r *aliyunBroker) Name() string {
	return "rocketmq_http"
}

func (r *aliyunBroker) Address() string {
	if len(r.nameServers) > 0 {
		return r.nameServers[0]
	} else if r.nameServerUrl != "" {
		return r.nameServerUrl
	}
	return defaultAddr
}

func (r *aliyunBroker) Options() broker.Options {
	return r.opts
}

func (r *aliyunBroker) Init(opts ...broker.Option) error {
	r.opts.Apply(opts...)

	if v, ok := r.opts.Context.Value(nameServersKey{}).([]string); ok {
		r.nameServers = v
	}
	if v, ok := r.opts.Context.Value(nameServerUrlKey{}).(string); ok {
		r.nameServerUrl = v
	}
	if v, ok := r.opts.Context.Value(accessKey{}).(string); ok {
		r.accessKey = v
	}
	if v, ok := r.opts.Context.Value(secretKey{}).(string); ok {
		r.secretKey = v
	}
	if v, ok := r.opts.Context.Value(securityTokenKey{}).(string); ok {
		r.securityToken = v
	}
	if v, ok := r.opts.Context.Value(retryCountKey{}).(int); ok {
		r.retryCount = v
	}
	if v, ok := r.opts.Context.Value(namespaceKey{}).(string); ok {
		r.namespace = v
	}
	if v, ok := r.opts.Context.Value(instanceNameKey{}).(string); ok {
		r.instanceName = v
	}
	if v, ok := r.opts.Context.Value(groupNameKey{}).(string); ok {
		r.groupName = v
	}

	return nil
}

func (r *aliyunBroker) Connect() error {
	r.RLock()
	if r.connected {
		r.RUnlock()
		return nil
	}
	r.RUnlock()

	endpoint := r.Address()
	client := aliyun.NewAliyunMQClient(endpoint, r.accessKey, r.secretKey, r.securityToken)
	r.client = client

	r.Lock()
	r.connected = true
	r.Unlock()

	return nil
}

func (r *aliyunBroker) Disconnect() error {
	r.RLock()
	if !r.connected {
		r.RUnlock()
		return nil
	}
	r.RUnlock()

	r.Lock()
	defer r.Unlock()

	r.client = nil
	r.connected = false

	return nil
}

func (r *aliyunBroker) Publish(topic string, msg broker.Any, opts ...broker.PublishOption) error {
	buf, err := broker.Marshal(r.opts.Codec, msg)
	if err != nil {
		return err
	}

	return r.publish(topic, buf, opts...)
}

func (r *aliyunBroker) publish(topic string, msg []byte, opts ...broker.PublishOption) error {
	options := broker.PublishOptions{
		Context: context.Background(),
	}
	for _, o := range opts {
		o(&options)
	}

	if r.client == nil {
		return errors.New("client is nil")
	}

	r.Lock()
	p, ok := r.producers[topic]
	if !ok {
		p = r.client.GetProducer(r.instanceName, topic)
		if p == nil {
			r.Unlock()
			return errors.New("create producer failed")
		}

		r.producers[topic] = p
	} else {
	}
	r.Unlock()

	aMsg := aliyun.PublishMessageRequest{
		MessageBody: string(msg),
	}

	if v, ok := options.Context.Value(propertiesKey{}).(map[string]string); ok {
		aMsg.Properties = v
	}
	if v, ok := options.Context.Value(delayTimeLevelKey{}).(int); ok {
		aMsg.StartDeliverTime = int64(v)
	}
	if v, ok := options.Context.Value(tagsKey{}).(string); ok {
		aMsg.MessageTag = v
	}
	if v, ok := options.Context.Value(keysKey{}).([]string); ok {
		var sb strings.Builder
		for _, k := range v {
			sb.WriteString(k)
			sb.WriteString(" ")
		}
		aMsg.MessageKey = sb.String()
	}
	if v, ok := options.Context.Value(shardingKeyKey{}).(string); ok {
		aMsg.ShardingKey = v
	}

	_, err := p.PublishMessage(aMsg)
	if err != nil {
		r.log.Errorf("[rocketmq]: send message error: %s\n", err)
		return err
	}

	return nil
}

// publishWithBackoffRetry 消息重发(指数退避重试算法)
// maxRetryCount 最大重试次数
// maxRetryTime 最大重试时间
// minDelay 初始重试时间
// factor 指数退避重试算法指数值
func (r *aliyunBroker) publishWithBackoffRetry(ctx context.Context, msg *aliyunPublication, consumeRetry *broker.ConsumeRetry, opts ...broker.PublishOption) (string, int64, error) {
	var firstRetryTime int64
	var retriedCount int64
	if consumeRetry == nil {
		return "", 0, errors.New("无法重试，请配置重试信息")
	}
	if len(msg.Message().Headers) > 0 {
		firstRetryTime = cast.ToInt64(msg.Message().Headers[FirstRetryTime])
		retriedCount = cast.ToInt64(msg.Message().Headers[RetriedCount])
	}
	b := backoff.NewBackOff(
		backoff.WithMinDelay(consumeRetry.MinDelay),
		backoff.WithMaxDelay(consumeRetry.MaxRetryTime),
		backoff.WithFactor(consumeRetry.Factor),
	)
	var i int64 = 0
	for i = 0; i < retriedCount; i++ {
		b.Duration()
	}
	delay := b.Duration()
	opts = append(opts, WithDelayTimeLevel(int(delay.Milliseconds())))

	retry := broker.NewRetry(firstRetryTime, retriedCount, consumeRetry.MaxRetryCount, consumeRetry.MaxRetryTime)
	err := retry.Do(func(firstRetryTime int64, retriedCount int64) error {
		m := map[string]string{
			FirstRetryTime: cast.ToString(firstRetryTime),
			RetriedCount:   cast.ToString(retriedCount),
		}
		opts = append(opts, WithProperties(m))
		err := r.Publish(msg.topic, msg.Message().Body, opts...)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, broker.ErrMaxRetryTime) || errors.Is(err, broker.ErrMaxRetryCount) {
			if consumeRetry.HandleRetryEnd != nil {
				consumeRetry.HandleRetryEnd(ctx, msg)
			}
		}
	}
	firstRetryTimeStr := time.Unix(retry.FirstRetryTime(), 0).Format("2006-01-02 15:04:05")
	return firstRetryTimeStr, retry.RetriedCount(), err
}

func (r *aliyunBroker) Subscribe(topic string, handler broker.Handler, binder broker.Binder, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	if r.client == nil {
		return nil, errors.New("client is nil")
	}

	options := broker.SubscribeOptions{
		Context: context.Background(),
		AutoAck: true,
		Queue:   r.groupName,
	}
	for _, o := range opts {
		o(&options)
	}
	if options.NumOfMessages < 3 {
		options.NumOfMessages = 3
	}

	mqConsumer := r.client.GetConsumer(r.instanceName, topic, options.Queue, options.MessageTag)

	sub := &aliyunSubscriber{
		opts:    options,
		topic:   topic,
		handler: handler,
		binder:  binder,
		reader:  mqConsumer,
		done:    make(chan struct{}),
	}

	go r.doConsume(sub)

	return sub, nil
}

func (r *aliyunBroker) doConsume(sub *aliyunSubscriber) {
	respChan := make(chan aliyun.ConsumeMessageResponse, 1)
	errChan := make(chan error, 1)

	pool, _ := ants.NewPoolWithFunc(sub.opts.NumOfMessages, func(rqMsg interface{}) {
		var traceId string
		hCtx := context.Background()
		spanCtx := trace.SpanContextFromContext(hCtx)
		h, _ := rqMsg.(handlerMessage)

		if len(h.AliyunPublication.Message().Headers) > 0 {
			traceId = h.AliyunPublication.Message().Headers["traceid"]
			if traceId != "" {
				traceID, err := trace.TraceIDFromHex(traceId)
				if err == nil {
					spanCtx = spanCtx.WithTraceID(traceID)
				}
				hCtx = trace.ContextWithSpanContext(hCtx, spanCtx)
			}
		}
		r.wrapHandler(hCtx, h, sub.handler)
	})

	go func() {
		for {
			// 长轮询消费消息，网络超时时间默认为35s。
			// 长轮询表示如果Topic没有消息，则客户端请求会在服务端挂起3s，3s内如果有消息可以消费则立即返回响应。
			// 一次最多消费3条（最多可设置为16条）
			// 长轮询时间3s（最多可设置为30s）
			sub.reader.ConsumeMessage(respChan, errChan, int32(sub.opts.NumOfMessages), 3)
		}
	}()

	go func() {
		defer pool.Release()

		for {
			select {
			case sub.done <- struct{}{}:
				log.Infof("consume message 接收退出信号...")
				return
			case resp := <-respChan:
				{
					var err error
					var m broker.Message
					var handles []string
					var count int
					h := handlerMessage{
						ResCh: make(chan handlerResult, sub.opts.NumOfMessages),
					}

					for _, msg := range resp.Messages {
						p := aliyunPublication{
							topic:  sub.topic,
							reader: sub.reader,
							m:      &m,
							rm:     []string{msg.ReceiptHandle},
							ctx:    r.opts.Context,
						}
						m.Headers = msg.Properties

						if sub.binder != nil {
							m.Body = sub.binder()
							if err := broker.Unmarshal(r.opts.Codec, []byte(msg.MessageBody), m.Body); err != nil {
								p.err = err
								r.log.Error(err)
							}
						} else {
							m.Body = []byte(msg.MessageBody)
						}

						h.Message = message{
							Key:           msg.MessageKey,
							Tag:           msg.MessageTag,
							ReceiptHandle: msg.ReceiptHandle,
						}
						h.AliyunPublication = p
						if err := pool.Invoke(h); err != nil {
							r.log.Errorf("提交消费处理任务失败 msg:%+v err:%v", msg, err)
							continue
						}
						count++
					}

					for i := 0; i < count; i++ {
						select {
						//case <-ticker.C:
						//	LogInfo("消费消息超时")
						case res := <-h.ResCh:
							// 消息消费失败:
							//   1.已配置重试策略，重试发布消息失败，不进行ack响应，依赖mq的重试
							//   2.未配置重试策略，不进行ack响应，依赖mq的重试
							if res.Err != nil {
								if sub.opts.ConsumeRetry != nil {
									opts := []broker.PublishOption{
										WithTag(res.Message.Tag),
										WithKeys([]string{res.Message.Key}),
									}
									firstRetryTime, retriedCount, err := r.publishWithBackoffRetry(res.Ctx, &res.AliyunPublication, sub.opts.ConsumeRetry, opts...)

									logMsg := "重试...%s msg:%+v 首次重试时间:%v 最大重试时间:%v 重试次数:%v 最大重试次数:%v"
									retrySuccess := fmt.Sprintf(logMsg, "mq发送完成", res.AliyunPublication, firstRetryTime, sub.opts.ConsumeRetry.MaxRetryTime, retriedCount, sub.opts.ConsumeRetry.MaxRetryCount)
									retryFail := fmt.Sprintf(logMsg+" err:%v", "mq发送失败", res.AliyunPublication, firstRetryTime, sub.opts.ConsumeRetry.MaxRetryTime, retriedCount, sub.opts.ConsumeRetry.MaxRetryCount, err)

									switch err {
									case nil:
										handles = append(handles, res.Message.ReceiptHandle)
										r.log.WithContext(res.Ctx).Infof(retrySuccess)
									case broker.ErrMaxRetryCount, broker.ErrMaxRetryTime:
										handles = append(handles, res.Message.ReceiptHandle)
										r.log.WithContext(res.Ctx).Errorf(retryFail)
									default:
										r.log.WithContext(res.Ctx).Errorf(retryFail)
									}
								}
							} else {
								// 提交任务成功，取消息句柄用于回复消息状态
								handles = append(handles, res.Message.ReceiptHandle)
							}
						}
					}
					close(h.ResCh)

					if sub.opts.AutoAck {
						if err = sub.reader.AckMessage(handles); err != nil {
							// 某些消息的句柄可能超时，会导致消息消费状态确认不成功。
							if errAckItems, ok := err.(gerr.ErrCode).Context()["Detail"].([]aliyun.ErrAckItem); ok {
								for _, errAckItem := range errAckItems {
									r.log.Errorf("ErrorHandle:%s, ErrorCode:%s, ErrorMsg:%s\n",
										errAckItem.ErrorHandle, errAckItem.ErrorCode, errAckItem.ErrorMsg)
								}
							} else {
								r.log.Error("ack err =", err)
							}
						}
					}
				}
			case err := <-errChan:
				{
					// Topic中没有消息可消费。
					if strings.Contains(err.(gerr.ErrCode).Error(), "MessageNotExist") {
						//r.log.Debug("No new message, continue!")
					} else {
						r.log.Error("获取MQ消息失败 err:%v", err)
						time.Sleep(time.Duration(3) * time.Second)
					}
				}
			case <-time.After(35 * time.Second):
				{
					//r.log.Debug("Timeout of consumer message ??")
				}
			}
		}
	}()
}

func (r *aliyunBroker) wrapHandler(ctx context.Context, h handlerMessage, handler broker.Handler) {
	err := handler(ctx, &h.AliyunPublication)
	res := handlerResult{
		Ctx:               ctx,
		Err:               err,
		AliyunPublication: h.AliyunPublication,
		Message:           h.Message,
	}
	h.ResCh <- res
}

type handlerMessage struct {
	AliyunPublication aliyunPublication
	Message           message
	ResCh             chan handlerResult
}

type handlerResult struct {
	Ctx               context.Context
	Err               error
	AliyunPublication aliyunPublication
	Message           message
}

type message struct {
	Tag           string
	Key           string
	ReceiptHandle string
}
