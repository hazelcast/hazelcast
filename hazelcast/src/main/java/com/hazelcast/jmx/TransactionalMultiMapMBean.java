package com.hazelcast.jmx;

import com.hazelcast.core.TransactionalMultiMap;

@ManagedDescription("TransactionalMultiMap")
public class TransactionalMultiMapMBean extends HazelcastMBean<TransactionalMultiMap> {

    protected TransactionalMultiMapMBean(TransactionalMultiMap managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = service.createObjectName("TransactionalMultiMap", managedObject.getName());
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the TransactionalMultiMap")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("size")
    @ManagedDescription("Name of the TransactionalMultiMap")
    public long size() {
        return managedObject.size();
    }
}
