package com.hazelcast.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.OperationService;

import java.util.Hashtable;


import static com.hazelcast.jmx.ManagementService.quote;
@ManagedDescription("HazelcastInstance.OperationService")
public class OperationServiceMBean extends HazelcastMBean<OperationService> {

    public OperationServiceMBean(HazelcastInstance hazelcastInstance, OperationService operationService, ManagementService service) {
        super(operationService, service);

        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("type", quote("HazelcastInstance.OperationService"));
        properties.put("name", quote("operationService" + hazelcastInstance.getName()));
        properties.put("HazelcastInstance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("responseQueueSize")
    @ManagedDescription("The size of the response queue")
    public int getResponseQueueSize(){
           return managedObject.getResponseQueueSize();
    }

    @ManagedAnnotation("operationExecutorQueueSize")
    @ManagedDescription("The size of the operation executor queue")
    int getOperationExecutorQueueSize(){
        return managedObject.getOperationExecutorQueueSize();
    }

    @ManagedAnnotation("runningOperationsCount")
    @ManagedDescription("the running operations count")
    public int getRunningOperationsCount(){
       return managedObject.getRunningOperationsCount();
    }

    @ManagedAnnotation("remoteOperationCount")
    @ManagedDescription("The number of remote operations")
    public int getRemoteOperationsCount(){
        return managedObject.getRemoteOperationsCount();
    }
}
