/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.monitor.LocalFlakeIdGeneratorStats;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.StatisticsAwareService;

public interface FlakeIdGeneratorService extends ManagedService, RemoteService,
                                                StatisticsAwareService<LocalFlakeIdGeneratorStats>, DynamicMetricsProvider {
    String SERVICE_NAME = "hz:impl:flakeIdGeneratorService";

    /**
     * Updated the statistics for the {@link FlakeIdGenerator} with the given
     * name for a newly generated batch of the given size.
     *
     * @param name name of the generator, not null
     * @param batchSize size of the batch created
     */
    void updateStatsForBatch(String name, int batchSize);
}
