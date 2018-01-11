/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.jmx;

import com.hazelcast.core.IExecutorService;


/**
 * Management bean for {@link com.hazelcast.core.IExecutorService}
 */
public class ExecutorServiceMBean extends HazelcastMBean<IExecutorService> {

    protected ExecutorServiceMBean(IExecutorService managedObject, ManagementService service) {
        super(managedObject, service);
        this.objectName = service.createObjectName("IExecutorService", managedObject.getName());
    }

    @ManagedAnnotation("localPendingTaskCount")
    @ManagedDescription("the number of pending operations of this executor service on this member")
    public long getLocalPendingTaskCount() {
        return managedObject.getLocalExecutorStats().getPendingTaskCount();
    }

    @ManagedAnnotation("localStartedTaskCount")
    @ManagedDescription(" the number of started operations of this executor service on this member")
    public long getLocalStartedTaskCount() {
        return managedObject.getLocalExecutorStats().getStartedTaskCount();
    }

    @ManagedAnnotation("localCompletedTaskCount")
    @ManagedDescription("the number of completed operations of this executor service on this member")
    public long getLocalCompletedTaskCount() {
        return managedObject.getLocalExecutorStats().getCompletedTaskCount();
    }

    @ManagedAnnotation("localCancelledTaskCount")
    @ManagedDescription("the number of cancelled operations of this executor service on this member")
    public long getLocalCancelledTaskCount() {
        return managedObject.getLocalExecutorStats().getCancelledTaskCount();
    }

    @ManagedAnnotation("localTotalStartLatency")
    @ManagedDescription("the total start latency of operations started of this executor on this member")
    public long getLocalTotalStartLatency() {
        return managedObject.getLocalExecutorStats().getTotalStartLatency();
    }

    @ManagedAnnotation("localTotalExecutionLatency")
    @ManagedDescription("the total execution time of operations finished of this executor on this member")
    public long getLocalTotalExecutionLatency() {
        return managedObject.getLocalExecutorStats().getTotalExecutionLatency();
    }

    @ManagedAnnotation("name")
    @ManagedDescription("")
    public String name() {
        return managedObject.getName();
    }
}
