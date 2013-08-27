package com.hazelcast.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

@ManagedDescription("HazelcastInstance.ManagedExecutorService")
public class ManagedExecutorServiceMBean extends HazelcastMBean<ManagedExecutorService> {

    public ManagedExecutorServiceMBean(HazelcastInstance hazelcastInstance, ManagedExecutorService executorService, ManagementService service) {
        super(executorService, service);

        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("type", quote("HazelcastInstance.ManagedExecutorService"));
        properties.put("name", quote(executorService.getName()));
        properties.put("HazelcastInstance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("name")
    @ManagedDescription("The name of the ManagedExecutor")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("queueSize")
    @ManagedDescription("The work queue size")
    public int getQueueSize() {
        return managedObject.queueSize();
    }

    @ManagedAnnotation("poolSize")
    @ManagedDescription("The current number of thread in the threadpool")
    public int getPoolSize() {
        return managedObject.poolSize();
    }

    @ManagedAnnotation("maxPoolSize")
    @ManagedDescription("The maximum number of thread in the threadpool")
    public int getMaxPoolSize() {
        return managedObject.maxPoolSize();
    }

    @ManagedAnnotation("isShutdown")
    @ManagedDescription("If the ManagedExecutor is shutdown")
    public boolean isShutdown() {
        return managedObject.isShutdown();
    }

    @ManagedAnnotation("isTerminated")
    @ManagedDescription("If the ManagedExecutor is terminated")
    public boolean isTerminated() {
        return managedObject.isTerminated();
    }

    @ManagedAnnotation("executedCount")
    @ManagedDescription("The number of tasks this ManagedExecutor has executed")
    public long getExecutedCount() {
        return managedObject.getExecutedCount();
    }
}

