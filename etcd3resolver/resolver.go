package etcd3resolver

import (
	etcd_cli "github.com/coreos/etcd/clientv3"
	"google.golang.org/grpc/resolver"
	"strings"
	"sync"
)

type etcdResolver struct {
	scheme        string
	etcdConfig    etcd_cli.Config
	etcdWatchPath string
	watcher       *Watcher
	cc            resolver.ClientConn
	wg            sync.WaitGroup
}

func (r *etcdResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	etcdCli, err := etcd_cli.New(r.etcdConfig)
	if err != nil {
		return nil, err
	}
	r.cc = cc
	r.watcher = newWatcher(r.etcdWatchPath, etcdCli)
	r.start()
	return r, nil
}

func (r *etcdResolver) Scheme() string {
	return r.scheme
}

func (r *etcdResolver) start() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		out := r.watcher.Watch()
		for addrs := range out {
			if addrs!=nil&&len(addrs) > 0 {
				r.cc.UpdateState(resolver.State{Addresses: addrs})
			}
		}
	}()
}

func (r *etcdResolver) ResolveNow(o resolver.ResolveNowOptions) {
}

func (r *etcdResolver) Close() {
	r.watcher.Close()
	r.wg.Wait()
}

func RegisterResolver(scheme string, etcdConfig etcd_cli.Config, registryDir, srvName string) {
	resolver.Register(&etcdResolver{
		scheme:        scheme,
		etcdConfig:    etcdConfig,
		etcdWatchPath: parseRegistryDir(registryDir) + "/" + srvName,
	})
}

func parseRegistryDir(registryDir string) string {
	if registryDir == "" {
		return ""
	}
	if strings.Contains(registryDir, "/") {
		arr:=strings.Split(registryDir, "/")
		for _,dir:=range arr{
			if dir!="" {
				return dir
			}
		}
		return ""
	} else {
		return registryDir
	}
}