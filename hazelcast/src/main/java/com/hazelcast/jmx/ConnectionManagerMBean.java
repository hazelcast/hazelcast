package com.hazelcast.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.ConnectionManager;

import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

@ManagedDescription("HazelcastInstance.ConnectionManager")
public class ConnectionManagerMBean  extends HazelcastMBean<ConnectionManager> {

    public ConnectionManagerMBean(HazelcastInstance hazelcastInstance, ConnectionManager connectionManager, ManagementService service) {
        super(connectionManager, service);

        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("type", quote("HazelcastInstance.ConnectionManager"));
        properties.put("instance", quote(hazelcastInstance.getName()));
        properties.put("name", hazelcastInstance.getName());
        setObjectName(properties);
    }

    public ConnectionManager getConnectionManager(){
        return managedObject;
    }

    @ManagedAnnotation("clientConnectionCount")
    @ManagedDescription("Current number of client connections")
    public int getCurrentClientConnections() {
        return getConnectionManager().getCurrentClientConnections();
    }

    @ManagedAnnotation("activeConnectionCount")
    @ManagedDescription("Current number of active connections")
    public int getActiveConnectionCount() {
        return getConnectionManager().getActiveConnectionCount();
    }

    @ManagedAnnotation("connectionCount")
    @ManagedDescription("Current number of connections")
    public int getConnectionCount() {
        return getConnectionManager().getConnectionCount();
    }
}
