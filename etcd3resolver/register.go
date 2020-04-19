package etcd3resolver

import (
	"encoding/json"
	etcd3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Registry struct {
	etcd3Client *etcd3.Client
	key         string
	value       map[string]interface{}
	ttl         time.Duration
	ctx         context.Context
	cancel      context.CancelFunc
	leaseID     etcd3.LeaseID
}

type Option struct {
	EtcdConfig  etcd3.Config
	RegistryDir string
	ServiceName string
	ServiceAddr string
	ServiceData map[string]interface{}
	Ttl         time.Duration
}

func NewRegistry(option Option) *Registry {
	client, err := etcd3.New(option.EtcdConfig)
	if err != nil {
		grpclog.Fatalln(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	registry := &Registry{
		etcd3Client: client,
		key:         option.RegistryDir + "/" + option.ServiceName + "/" + option.ServiceAddr,
		value:       option.ServiceData,
		ttl:         option.Ttl / time.Second,
		ctx:         ctx,
		cancel:      cancel,
	}
	return registry
}

func (e *Registry) put()  {
	resp, err := e.etcd3Client.Grant(e.ctx, int64(e.ttl))
	if err != nil {
		grpclog.Fatalln(err)
	}
	e.leaseID=resp.ID
	_, err = e.etcd3Client.Get(e.ctx, e.key)
	value := ""
	if e.value != nil {
		if valueByte, errjson := json.Marshal(e.value); errjson == nil {
			value = string(valueByte)
		}else {
			grpclog.Infoln(errjson)
		}
	}
	if err != nil {
		if err == rpctypes.ErrKeyNotFound {
			if _, err := e.etcd3Client.Put(e.ctx, e.key, value, etcd3.WithLease(resp.ID)); err != nil {
				grpclog.Fatalln(err.Error())
			}
		} else {
			grpclog.Fatalln(err.Error())
		}
	} else {
		if _, err := e.etcd3Client.Put(e.ctx, e.key, value, etcd3.WithLease(resp.ID)); err != nil {
			grpclog.Fatalln(err.Error())
		}
	}
}


func (e *Registry)delete()   {
	if _, err := e.etcd3Client.Delete(context.Background(), e.key); err != nil {
		grpclog.Infoln(err.Error())
	}
	return
}


func (e *Registry)keepalive()   {
	_, err := e.etcd3Client.KeepAliveOnce(e.ctx, e.leaseID)
	if err != nil {
		grpclog.Infoln(err.Error())
	}
}


func (e *Registry) Register()  {
    e.put()
	go func() {
		ticker := time.NewTicker(e.ttl / 5)
		for {
			select {
			case <-ticker.C:
				e.keepalive()
			case <-e.ctx.Done():
				e.delete()
				return
			}
		}
	}()

	go func() {
		stopSignal:=make(chan os.Signal,1)
		signal.Notify(stopSignal,syscall.SIGTERM,syscall.SIGINT,syscall.SIGKILL,syscall.SIGHUP,syscall.SIGQUIT)
		s:=<-stopSignal
		grpclog.Infoln("receive signal ",s)
		e.UnRegister()
		os.Exit(1)
	}()

}

func (e *Registry) UnRegister()  {
	e.cancel()
	return
}

