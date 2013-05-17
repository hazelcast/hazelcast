package com.hazelcast.client.config;

import com.hazelcast.client.spi.ClientProxyFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @mdogan 5/17/13
 */
public class ProxyFactoryConfig {

    private final Map<String, ClientProxyFactory> factoryMap = new HashMap<String, ClientProxyFactory>();

    public ProxyFactoryConfig addProxyFactory(String service, ClientProxyFactory factory) {
        factoryMap.put(service, factory);
        return this;
    }

    public Map<String, ClientProxyFactory> getFactories() {
        return factoryMap;
    }
}
