/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.executor.ManagedExecutorService;

import java.util.Map;

import static com.hazelcast.internal.jmx.ManagementService.quote;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Management bean for {@link com.hazelcast.internal.util.executor.ManagedExecutorService}
 */
@ManagedDescription("HazelcastInstance.ManagedExecutorService")
public class ManagedExecutorServiceMBean extends HazelcastMBean<ManagedExecutorService> {
    private static final int INITIAL_CAPACITY = 3;

    public ManagedExecutorServiceMBean(HazelcastInstance hazelcastInstance, ManagedExecutorService executorService,
                                       ManagementService service) {
        super(executorService, service);

        final Map<String, String> properties = createHashMap(INITIAL_CAPACITY);
        properties.put("type", quote("HazelcastInstance.ManagedExecutorService"));
        properties.put("name", quote(executorService.getName()));
        properties.put("instance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("name")
    @ManagedDescription("The name of the ManagedExecutor")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("queueSize")
    @ManagedDescription("The work queue size")
    public int queueSize() {
        return managedObject.getQueueSize();
    }

    @ManagedAnnotation("poolSize")
    @ManagedDescription("The current number of thread in the threadpool")
    public int poolSize() {
        return managedObject.getPoolSize();
    }

    @ManagedAnnotation("remainingQueueCapacity")
    @ManagedDescription("The remaining capacity on the work queue")
    public int queueRemainingCapacity() {
        return managedObject.getRemainingQueueCapacity();
    }

    @ManagedAnnotation("maximumPoolSize")
    @ManagedDescription("The maximum number of thread in the threadpool")
    public int maxPoolSize() {
        return managedObject.getMaximumPoolSize();
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

    @ManagedAnnotation("completedTaskCount")
    @ManagedDescription("The number of tasks this ManagedExecutor has executed")
    public long getExecutedCount() {
        return managedObject.getCompletedTaskCount();
    }
}

