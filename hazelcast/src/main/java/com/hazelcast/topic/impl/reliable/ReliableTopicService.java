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

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.MapUtil;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

public class ReliableTopicService implements ManagedService, RemoteService, StatisticsAwareService {

    public static final String SERVICE_NAME = "hz:impl:reliableTopicService";
    private final ConcurrentMap<String, LocalTopicStatsImpl> statsMap = new ConcurrentHashMap<String, LocalTopicStatsImpl>();
    private final ConstructorFunction<String, LocalTopicStatsImpl> localTopicStatsConstructorFunction =
            new ConstructorFunction<String, LocalTopicStatsImpl>() {
                public LocalTopicStatsImpl createNew(String mapName) {
                    return new LocalTopicStatsImpl();
                }
            };

    private final NodeEngine nodeEngine;

    public ReliableTopicService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        ReliableTopicConfig topicConfig = nodeEngine.getConfig().findReliableTopicConfig(objectName);
        return new ReliableTopicProxy(objectName, nodeEngine, this, topicConfig);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        statsMap.remove(objectName);
    }

    /**
     * Returns reliable topic statistics local to this member
     * for the reliable topic with {@code name}.
     *
     * @param name the name of the reliable topic
     * @return the statistics local to this member
     */
    public LocalTopicStatsImpl getLocalTopicStats(String name) {
        return getOrPutSynchronized(statsMap, name, statsMap, localTopicStatsConstructorFunction);
    }

    @Override
    public Map<String, LocalTopicStats> getStats() {
        Map<String, LocalTopicStats> topicStats = MapUtil.createHashMap(statsMap.size());
        for (Map.Entry<String, LocalTopicStatsImpl> queueStat : statsMap.entrySet()) {
            topicStats.put(queueStat.getKey(), queueStat.getValue());
        }
        return topicStats;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
        statsMap.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }
}
