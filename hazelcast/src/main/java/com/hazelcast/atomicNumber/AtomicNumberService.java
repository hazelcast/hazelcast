package com.hazelcast.atomicNumber;

import com.hazelcast.atomicNumber.proxy.AtomicNumberProxy;
import com.hazelcast.config.Config;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ServiceProxy;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// author: sancar - 21.12.2012
public class AtomicNumberService implements ManagedService , RemoteService{

    public static final String NAME = "hz:impl:atomicNumberService";

    private NodeEngine nodeEngine;

    private final Map<String, Long> numbers = new HashMap<String, Long>();

    private final ConcurrentMap<String, AtomicNumberProxy> proxies = new ConcurrentHashMap<String, AtomicNumberProxy>();


    public AtomicNumberService(){

    }

    public long getNumber(String name) {
        Long value = numbers.get(name);
        if(value == null){
            value = Long.valueOf(0);
            numbers.put(name,value);
        }
        return value;
    }

    public void setNumber(String name, long newValue) {
        numbers.put(name,newValue);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public void destroy() {

    }

    public Config getConfig(){
        return nodeEngine.getConfig();
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
        return new HashSet<ServiceProxy>(proxies.values());
    }
}
