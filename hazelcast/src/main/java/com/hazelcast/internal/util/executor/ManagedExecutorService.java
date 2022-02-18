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

package com.hazelcast.internal.util.executor;

import com.hazelcast.internal.metrics.Probe;

import java.util.concurrent.ExecutorService;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_COMPLETED_TASKS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_MAXIMUM_POOL_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_POOL_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_QUEUE_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_REMAINING_QUEUE_CAPACITY;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;

public interface ManagedExecutorService extends ExecutorService {

    String getName();

    @Probe(name = EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_COMPLETED_TASKS)
    long getCompletedTaskCount();

    @Probe(name = EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_MAXIMUM_POOL_SIZE)
    int getMaximumPoolSize();

    @Probe(name = EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_POOL_SIZE)
    int getPoolSize();

    @Probe(name = EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_QUEUE_SIZE, level = MANDATORY)
    int getQueueSize();

    @Probe(name = EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_REMAINING_QUEUE_CAPACITY)
    int getRemainingQueueCapacity();
}
