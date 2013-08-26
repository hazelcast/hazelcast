package com.hazelcast.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.ProxyService;

import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

@ManagedDescription("HazelcastInstance.ProxyService")
public class ProxyServiceMBean extends HazelcastMBean<ProxyService> {

    public ProxyServiceMBean(HazelcastInstance hazelcastInstance, ProxyService proxyService, ManagementService service) {
        super(proxyService, service);

        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("type", quote("HazelcastInstance.ProxyService"));
        properties.put("name", quote("proxyService" + hazelcastInstance.getName()));
        properties.put("HazelcastInstance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }


    @ManagedAnnotation("proxyCount")
    @ManagedDescription("The number proxies")
    public int getProxyCount(){
        return managedObject.getProxyCount();
    }
}