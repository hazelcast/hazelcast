package com.hazelcast.jmx;

import com.hazelcast.core.IAtomicReference;

@ManagedDescription("IAtomicReference")
public class AtomicReferenceMBean extends HazelcastMBean<IAtomicReference> {

    public AtomicReferenceMBean(IAtomicReference managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = service.createObjectName("IAtomicReference",managedObject.getName());
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the DistributedObject")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("partitionKey")
    @ManagedDescription("the partitionKey")
    public String getPartitionKey() {
        return managedObject.getPartitionKey();
    }
}

