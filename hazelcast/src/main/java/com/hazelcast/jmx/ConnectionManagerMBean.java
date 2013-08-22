package com.hazelcast.jmx;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.ConnectionManager;

@ManagedDescription("ConnectionManager")
public class ConnectionManagerMBean  extends HazelcastMBean<ConnectionManager> {

    public ConnectionManagerMBean(ConnectionManager connectionManager, ManagementService service) {
        super(connectionManager, service);

        objectName = service.createObjectName("ConnectionManager", "ConnectionManager-"+connectionManager.hashCode());

    }

    @ManagedAnnotation("clientConnectionCount")
    @ManagedDescription("Current number of client connections")
    public int getCurrentClientConnections() {
        return managedObject.getCurrentClientConnections();
    }
}
