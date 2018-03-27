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

package com.hazelcast.flakeidgen.impl;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.monitor.LocalFlakeIdGeneratorStats;
import com.hazelcast.monitor.impl.LocalFlakeIdGeneratorStatsImpl;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.util.ConstructorFunction;

public class FlakeIdGeneratorService implements ManagedService, RemoteService,
        StatisticsAwareService<LocalFlakeIdGeneratorStats> {

    public static final String SERVICE_NAME = "hz:impl:flakeIdGeneratorService";

    private NodeEngine nodeEngine;
    private final ConcurrentHashMap<String, LocalFlakeIdGeneratorStatsImpl> statsMap
        = new ConcurrentHashMap<String, LocalFlakeIdGeneratorStatsImpl>();
    private final ConstructorFunction<String, LocalFlakeIdGeneratorStatsImpl> localFlakeIdStatsConstructorFunction
        = new ConstructorFunction<String, LocalFlakeIdGeneratorStatsImpl>() {
        public LocalFlakeIdGeneratorStatsImpl createNew(String key) {
            return new LocalFlakeIdGeneratorStatsImpl();
        }
    };

    public FlakeIdGeneratorService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {
        statsMap.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        return new FlakeIdGeneratorProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
        statsMap.remove(name);
    }

    @Override
    public Map<String, LocalFlakeIdGeneratorStats> getStats() {
        return new HashMap<String, LocalFlakeIdGeneratorStats>(statsMap);
    }

    /**
     * Updated the statistics for the {@link FlakeIdGenerator} with the given
     * name for a newly generated batch of the given size.
     *
     * @param name name of the generator, not null
     * @param batchSize size of the batch created
     */
    public void updateStatsForBatch(String name, int batchSize) {
        getLocalFlakeIdStats(name).update(batchSize);
    }

    private LocalFlakeIdGeneratorStatsImpl getLocalFlakeIdStats(String name) {
        return getOrPutIfAbsent(statsMap, name, localFlakeIdStatsConstructorFunction);
    }
}
