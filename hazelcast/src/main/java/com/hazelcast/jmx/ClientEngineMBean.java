package com.hazelcast.jmx;

import com.hazelcast.client.ClientEngine;
import com.hazelcast.core.HazelcastInstance;

import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

@ManagedDescription("HazelcastInstance.ClientEngine")
public class ClientEngineMBean extends HazelcastMBean<ClientEngine> {

    public ClientEngineMBean(HazelcastInstance hazelcastInstance, ClientEngine clientEngine, ManagementService service) {
        super(clientEngine, service);

        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("type", quote("HazelcastInstance.ClientEngine"));
        properties.put("name", quote(hazelcastInstance.getName()));
        properties.put("HazelcastInstance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("clientEndpointCount")
    @ManagedDescription("The number of client endpoints")
     public int getClientEndpointCount() {
        return managedObject.getClientEndpointCount();
    }
}
