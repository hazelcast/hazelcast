package com.hazelcast.atomicNumber;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.proxy.DataMapProxy;
import com.hazelcast.map.proxy.MapProxy;
import com.hazelcast.map.proxy.ObjectMapProxy;
import com.hazelcast.queue.QueueContainer;
import com.hazelcast.queue.proxy.QueueProxy;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ServiceProxy;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// author: sancar - 21.12.2012
public class AtomicNumberService implements ManagedService , RemoteService {

    private NodeEngine nodeEngine;

    public static final String NAME = "hz:impl:atomicNumberService";

    private final ConcurrentMap<String, AtomicNumberProxy> proxies = new ConcurrentHashMap<String, AtomicNumberProxy>();



    public AtomicNumberService(){

    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public void destroy() {

    }

    public ServiceProxy getProxy(Object... params) {
        final String name = String.valueOf(params[0]);
        if (params.length > 1 && Boolean.TRUE.equals(params[1])) {
            return new AtomicNumberProxy(name, nodeEngine);
        }
        AtomicNumberProxy proxy = proxies.get(name);
        if (proxy == null) {
            proxy = new AtomicNumberProxy(name, nodeEngine);
            final AtomicNumberProxy currentProxy = proxies.putIfAbsent(name, proxy);
            proxy = currentProxy != null ? currentProxy : proxy;
        }
        return proxy;
    }

    public Collection<ServiceProxy> getProxies() {
        return null;
    }
}
