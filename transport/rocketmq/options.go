package rocketmq

import (
	"crypto/tls"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-kratos/kratos/v2/encoding"
	"github.com/go-kratos/kratos/v2/log"

	"github.com/tx7do/kratos-transport/broker"
	"github.com/tx7do/kratos-transport/broker/rocketmq"
)

type ServerOption func(o *Server)

func WithTLSConfig(c *tls.Config) ServerOption {
	return func(s *Server) {
		if c != nil {
			s.brokerOpts = append(s.brokerOpts, broker.WithEnableSecure(true))
		}
		s.brokerOpts = append(s.brokerOpts, broker.WithTLSConfig(c))
	}
}

func WithLogger(logger log.Logger) ServerOption {
	return func(s *Server) {
		s.log = log.NewHelper(logger)
		s.brokerOpts = append(s.brokerOpts, broker.WithLogger(s.log))
	}
}

func WithAliyunHttpSupport() ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, rocketmq.WithAliyunHttpSupport())
	}
}

func WithEnableTrace() ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, rocketmq.WithEnableTrace())
	}
}

func WithNameServer(addrs []string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, rocketmq.WithNameServer(addrs))
	}
}

func WithNameServerDomain(uri string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, rocketmq.WithNameServerDomain(uri))
	}
}

func WithCredentials(accessKey, secretKey, securityToken string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, rocketmq.WithAccessKey(accessKey))
		s.brokerOpts = append(s.brokerOpts, rocketmq.WithSecretKey(secretKey))
		s.brokerOpts = append(s.brokerOpts, rocketmq.WithSecurityToken(securityToken))
	}
}

func WithNamespace(ns string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, rocketmq.WithNamespace(ns))
	}
}

func WithInstanceName(name string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, rocketmq.WithInstanceName(name))
	}
}

func WithGroupName(name string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, rocketmq.WithGroupName(name))
	}
}

func WithRetryCount(count int) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, rocketmq.WithRetryCount(count))
	}
}

func WithCodec(c encoding.Codec) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, broker.WithCodec(c))
	}
}

func WithGlobalTracerProvider() ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, broker.WithGlobalTracerProvider())
	}
}

func WithGlobalPropagator() ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, broker.WithGlobalPropagator())
	}
}

func WithTracerProvider(provider trace.TracerProvider, tracerName string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, broker.WithTracerProvider(provider, tracerName))
	}
}

func WithPropagator(propagators propagation.TextMapPropagator) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, broker.WithPropagator(propagators))
	}
}

func WithCallBefore(before func(message broker.Message)) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, broker.WithCallBefore(before))
	}
}

func WithCallAfter(after func(message broker.Message, handleErr error)) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, broker.WithCallAfter(after))
	}
}
