/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.OperationService;
import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

/**
 * Management bean for {@link com.hazelcast.spi.OperationService}
 */
@ManagedDescription("HazelcastInstance.OperationService")
public class OperationServiceMBean extends HazelcastMBean<OperationService> {

    private static final int INITIAL_CAPACITY = 3;

    public OperationServiceMBean(HazelcastInstance hazelcastInstance, OperationService operationService,
                                 ManagementService service) {
        super(operationService, service);

        Hashtable<String, String> properties = new Hashtable<String, String>(INITIAL_CAPACITY);
        properties.put("type", quote("HazelcastInstance.OperationService"));
        properties.put("name", quote("operationService" + hazelcastInstance.getName()));
        properties.put("instance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("responseQueueSize")
    @ManagedDescription("The size of the response queue")
    public int getResponseQueueSize() {
        return managedObject.getResponseQueueSize();
    }

    @ManagedAnnotation("operationExecutorQueueSize")
    @ManagedDescription("The size of the operation executor queue")
    int getOperationExecutorQueueSize() {
        return managedObject.getOperationExecutorQueueSize();
    }

    @ManagedAnnotation("runningOperationsCount")
    @ManagedDescription("the running operations count")
    public int getRunningOperationsCount() {
        return managedObject.getRunningOperationsCount();
    }

    @ManagedAnnotation("remoteOperationCount")
    @ManagedDescription("The number of remote operations")
    public int getRemoteOperationsCount() {
        return managedObject.getRemoteOperationsCount();
    }

    @ManagedAnnotation("executedOperationCount")
    @ManagedDescription("The number of executed operations")
    public long getExecutedOperationCount() {
        return managedObject.getExecutedOperationCount();
    }

    @ManagedAnnotation("operationThreadCount")
    @ManagedDescription("Number of threads executing operations")
    public long getOperationThreadCount() {
        return managedObject.getPartitionOperationThreadCount();
    }
}
