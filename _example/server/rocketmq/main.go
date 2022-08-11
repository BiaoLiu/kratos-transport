package main

import (
	"context"
	"fmt"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/encoding"
	"github.com/tx7do/kratos-transport/broker"
	"github.com/tx7do/kratos-transport/transport/rocketmq"
)

const (
	testEndpoint     = "http://aliyuncs.com"
	testInstanceId   = "1684"
	testGroupId      = "GID"
	testAccessKey    = "LTAI"
	testAccessSecret = "CWIL"
	testTopic        = "Con"
)

type user struct {
	UserId int64 `json:"userid"`
}

func main() {
	opts := []rocketmq.ServerOption{
		rocketmq.WithNameServerDomain(testEndpoint),
		rocketmq.WithInstanceName(testInstanceId),
		rocketmq.WithGroupName(testGroupId),
		rocketmq.WithCredentials(testAccessKey, testAccessSecret, ""),
		rocketmq.WithAliyunHttpSupport(),
		rocketmq.WithCodec(encoding.GetCodec("json")),
		//rocketmq.WithLogger(),
	}
	rocketmqSrv := rocketmq.NewServer(opts...)

	_ = rocketmqSrv.RegisterSubscriber(context.Background(), testTopic, testGroupId, func(ctx context.Context, event broker.Event) error {
		fmt.Println("handle...")
		return nil
	}, func() broker.Any {
		return &user{}
	})
	//defer rocketmqSrv.Disconnect()

	app := kratos.New(
		kratos.Name("rocketmq"),
		kratos.Server(
			rocketmqSrv,
		),
	)
	if err := app.Run(); err != nil {
		panic(err)
	}
}
