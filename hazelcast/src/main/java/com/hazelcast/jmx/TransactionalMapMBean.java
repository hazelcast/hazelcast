package com.hazelcast.jmx;

import com.hazelcast.core.TransactionalMap;

@ManagedDescription("TransactionalMap")
public class TransactionalMapMBean extends HazelcastMBean<TransactionalMap> {

    protected TransactionalMapMBean(TransactionalMap managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = service.createObjectName("TransactionalMap", managedObject.getName());
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the TransactionalMap")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("size")
    @ManagedDescription("Name of the TransactionalMap")
    public long size() {
        return managedObject.size();
    }
}
