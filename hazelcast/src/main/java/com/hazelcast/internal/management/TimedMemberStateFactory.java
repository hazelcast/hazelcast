/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.impl.CacheDistributedObject;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.Member;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.monitor.LocalOperationStats;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.monitor.impl.LocalCacheStatsImpl;
import com.hazelcast.monitor.impl.LocalMemoryStatsImpl;
import com.hazelcast.monitor.impl.LocalOperationStatsImpl;
import com.hazelcast.monitor.impl.MemberPartitionStateImpl;
import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.topic.impl.TopicService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A Factory for creating {@link com.hazelcast.monitor.TimedMemberState} instances.
 */
public class TimedMemberStateFactory {

    private static final int INITIAL_PARTITION_SAFETY_CHECK_DELAY = 15;
    private static final int PARTITION_SAFETY_CHECK_PERIOD = 60;

    private final HazelcastInstanceImpl instance;
    private final int maxVisibleInstanceCount;
    private final boolean cacheServiceEnabled;
    private final ILogger logger;

    private volatile boolean memberStateSafe = true;

    public TimedMemberStateFactory(HazelcastInstanceImpl instance) {
        this.instance = instance;

        Node node = instance.node;
        maxVisibleInstanceCount = node.groupProperties.MC_MAX_INSTANCE_COUNT.getInteger();
        cacheServiceEnabled = node.nodeEngine.getService(CacheService.SERVICE_NAME) != null;
        logger = node.getLogger(TimedMemberStateFactory.class);
    }

    public void init() {
        instance.node.nodeEngine.getExecutionService().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                memberStateSafe = instance.getPartitionService().isLocalMemberSafe();
            }
        }, INITIAL_PARTITION_SAFETY_CHECK_DELAY, PARTITION_SAFETY_CHECK_PERIOD, TimeUnit.SECONDS);
    }

    public TimedMemberState createTimedMemberState() {
        MemberStateImpl memberState = new MemberStateImpl();
        Collection<StatisticsAwareService> services = instance.node.nodeEngine.getServices(StatisticsAwareService.class);
        createMemberState(memberState, services);

        TimedMemberState timedMemberState = new TimedMemberState();
        timedMemberState.setMaster(instance.node.isMaster());
        timedMemberState.setMemberList(new ArrayList<String>());
        if (timedMemberState.getMaster()) {
            Set<Member> memberSet = instance.getCluster().getMembers();
            for (Member member : memberSet) {
                MemberImpl memberImpl = (MemberImpl) member;
                Address address = memberImpl.getAddress();
                timedMemberState.getMemberList().add(address.getHost() + ":" + address.getPort());
            }
        }
        timedMemberState.setMemberState(memberState);
        GroupConfig groupConfig = instance.getConfig().getGroupConfig();
        timedMemberState.setClusterName(groupConfig.getName());
        timedMemberState.setInstanceNames(getLongInstanceNames(services));

        return timedMemberState;
    }

    protected LocalMemoryStats getMemoryStats() {
        return new LocalMemoryStatsImpl(instance.getMemoryStats());
    }

    protected LocalOperationStats getOperationStats() {
        return new LocalOperationStatsImpl(instance.node);
    }

    private void createMemberState(MemberStateImpl memberState, Collection<StatisticsAwareService> services) {
        Node node = instance.node;

        HashSet<ClientEndPointDTO> serializableClientEndPoints = new HashSet<ClientEndPointDTO>();
        for (Client client : instance.node.clientEngine.getClients()) {
            serializableClientEndPoints.add(new ClientEndPointDTO(client));
        }
        memberState.setClients(serializableClientEndPoints);

        Address thisAddress = node.getThisAddress();
        memberState.setAddress(thisAddress.getHost() + ":" + thisAddress.getPort());
        TimedMemberStateFactoryHelper.registerJMXBeans(instance, memberState);

        MemberPartitionStateImpl memberPartitionState = (MemberPartitionStateImpl) memberState.getMemberPartitionState();
        InternalPartitionService partitionService = node.getPartitionService();
        InternalPartition[] partitions = partitionService.getPartitions();

        List<Integer> partitionList = memberPartitionState.getPartitions();
        for (InternalPartition partition : partitions) {
            if (partition.isLocal()) {
                partitionList.add(partition.getPartitionId());
            }
        }
        memberPartitionState.setMigrationQueueSize(partitionService.getMigrationQueueSize());
        memberPartitionState.setMemberStateSafe(memberStateSafe);

        memberState.setLocalMemoryStats(getMemoryStats());
        memberState.setOperationStats(getOperationStats());
        TimedMemberStateFactoryHelper.createRuntimeProps(memberState);
        createMemState(memberState, services);
    }

    private void createMemState(MemberStateImpl memberState, Collection<StatisticsAwareService> services) {
        int count = 0;
        Config config = instance.getConfig();

        for (StatisticsAwareService service : services) {
            if (count < maxVisibleInstanceCount) {
                if (service instanceof MapService) {
                    count = handleMap(memberState, count, config, ((MapService) service).getStats());
                } else if (service instanceof MultiMapService) {
                    count = handleMultimap(memberState, count, config, ((MultiMapService) service).getStats());
                } else if (service instanceof QueueService) {
                    count = handleQueue(memberState, count, config, ((QueueService) service).getStats());
                } else if (service instanceof TopicService) {
                    count = handleTopic(memberState, count, config, ((TopicService) service).getStats());
                } else if (service instanceof DistributedExecutorService) {
                    count = handleExecutorService(memberState, count, config, ((DistributedExecutorService) service).getStats());
                }
            }
        }

        if (cacheServiceEnabled) {
            ICacheService cacheService = getCacheService();
            for (CacheConfig cacheConfig : cacheService.getCacheConfigs()) {
                if (cacheConfig.isStatisticsEnabled()) {
                    CacheStatistics statistics = cacheService.getStatistics(cacheConfig.getNameWithPrefix());
                    count = handleCache(memberState, count, cacheConfig, statistics);
                }
            }
        }
    }

    private int handleExecutorService(MemberStateImpl memberState, int count, Config config,
                                      Map<String, LocalExecutorStats> executorServices) {

        for (Map.Entry<String, LocalExecutorStats> entry : executorServices.entrySet()) {
            String name = entry.getKey();
            if (count >= maxVisibleInstanceCount) {
                break;
            } else if (config.findExecutorConfig(name).isStatisticsEnabled()) {
                LocalExecutorStats stats = entry.getValue();
                memberState.putLocalExecutorStats(name, stats);
                ++count;
            }
        }
        return count;
    }

    private int handleMultimap(MemberStateImpl memberState, int count, Config config, Map<String, LocalMultiMapStats> multiMaps) {
        for (Map.Entry<String, LocalMultiMapStats> entry : multiMaps.entrySet()) {
            String name = entry.getKey();
            if (count >= maxVisibleInstanceCount) {
                break;
            } else if (config.findMultiMapConfig(name).isStatisticsEnabled()) {
                LocalMultiMapStats stats = entry.getValue();
                memberState.putLocalMultiMapStats(name, stats);
                ++count;
            }
        }
        return count;
    }

    private int handleTopic(MemberStateImpl memberState, int count, Config config, Map<String, LocalTopicStats> topics) {
        for (Map.Entry<String, LocalTopicStats> entry : topics.entrySet()) {
            String name = entry.getKey();
            if (count >= maxVisibleInstanceCount) {
                break;
            } else if (config.findTopicConfig(name).isStatisticsEnabled()) {
                LocalTopicStats stats = entry.getValue();
                memberState.putLocalTopicStats(name, stats);
                ++count;
            }
        }
        return count;
    }

    private int handleQueue(MemberStateImpl memberState, int count, Config config, Map<String, LocalQueueStats> queues) {
        for (Map.Entry<String, LocalQueueStats> entry : queues.entrySet()) {
            String name = entry.getKey();
            if (count >= maxVisibleInstanceCount) {
                break;
            } else if (config.findQueueConfig(name).isStatisticsEnabled()) {
                LocalQueueStats stats = entry.getValue();
                memberState.putLocalQueueStats(name, stats);
                ++count;
            }
        }
        return count;
    }

    private int handleMap(MemberStateImpl memberState, int count, Config config, Map<String, LocalMapStats> maps) {
        for (Map.Entry<String, LocalMapStats> entry : maps.entrySet()) {
            String name = entry.getKey();
            if (count >= maxVisibleInstanceCount) {
                break;
            } else if (config.findMapConfig(name).isStatisticsEnabled()) {
                LocalMapStats stats = entry.getValue();
                memberState.putLocalMapStats(name, stats);
                ++count;
            }
        }
        return count;
    }

    private int handleCache(MemberStateImpl memberState, int count, CacheConfig config, CacheStatistics cacheStatistics) {
        memberState.putLocalCacheStats(config.getNameWithPrefix(), new LocalCacheStatsImpl(cacheStatistics));
        return count + 1;
    }

    private Set<String> getLongInstanceNames(Collection<StatisticsAwareService> services) {
        Set<String> setLongInstanceNames = new HashSet<String>(maxVisibleInstanceCount);
        collectInstanceNames(setLongInstanceNames, services);
        return setLongInstanceNames;
    }

    private void collectInstanceNames(Set<String> setLongInstanceNames, Collection<StatisticsAwareService> services) {
        int count = 0;
        Config config = instance.getConfig();
        for (StatisticsAwareService service : services) {
            if (count < maxVisibleInstanceCount) {
                if (service instanceof MapService) {
                    count = collectMapName(setLongInstanceNames, count, config, service.getStats().keySet());
                } else if (service instanceof MultiMapService) {
                    count = collectMultiMapName(setLongInstanceNames, count, config, service.getStats().keySet());
                } else if (service instanceof QueueService) {
                    count = collectQueueName(setLongInstanceNames, count, config, service.getStats().keySet());
                } else if (service instanceof TopicService) {
                    count = collectTopicName(setLongInstanceNames, count, config, service.getStats().keySet());
                } else if (service instanceof DistributedExecutorService) {
                    count = collectExecutorServiceName(setLongInstanceNames, count, config, service.getStats().keySet());
                } else {
                    logger.finest("Statistics service ignored for monitoring: " + service.getClass().getName());
                }
            } else {
                break;
            }
        }

        if (cacheServiceEnabled) {
            for (CacheConfig cacheConfig : getCacheService().getCacheConfigs()) {
                if (cacheConfig.isStatisticsEnabled()) {
                    count = collectCacheName(setLongInstanceNames, count, cacheConfig);
                }
            }
        }

    }

    private int collectExecutorServiceName(Set<String> setLongInstanceNames, int count, Config config,
                                           Set<String> executorServiceNames) {
        for (String name : executorServiceNames) {
            if (config.findExecutorConfig(name).isStatisticsEnabled() && count < maxVisibleInstanceCount) {
                setLongInstanceNames.add("e:" + name);
                ++count;
            }
        }
        return count;

    }

    private int collectTopicName(Set<String> setLongInstanceNames, int count, Config config, Set<String> topicNames) {
        for (String name : topicNames) {
            if (config.findTopicConfig(name).isStatisticsEnabled() && count < maxVisibleInstanceCount) {
                setLongInstanceNames.add("t:" + name);
                ++count;
            }
        }
        return count;
    }

    private int collectQueueName(Set<String> setLongInstanceNames, int count, Config config, Set<String> queueNames) {
        for (String name : queueNames) {
            if (config.findQueueConfig(name).isStatisticsEnabled() && count < maxVisibleInstanceCount) {
                setLongInstanceNames.add("q:" + name);
                ++count;
            }
        }
        return count;
    }

    private int collectMapName(Set<String> setLongInstanceNames, int count, Config config, Set<String> mapNames) {
        for (String name : mapNames) {
            if (config.findMapConfig(name).isStatisticsEnabled() && count < maxVisibleInstanceCount) {
                setLongInstanceNames.add("c:" + name);
                ++count;
            }
        }
        return count;
    }

    private int collectCacheName(Set<String> setLongInstanceNames, int count, CacheConfig config) {
        if (config.isStatisticsEnabled()) {
            setLongInstanceNames.add("j:" + config.getNameWithPrefix());
            return count + 1;
        }
        return count;
    }

    private int collectMultiMapName(Set<String> setLongInstanceNames, int count, Config config, Set<String> multiMapNames) {
        for (String name : multiMapNames) {
            if (config.findMultiMapConfig(name).isStatisticsEnabled() && count < maxVisibleInstanceCount) {
                setLongInstanceNames.add("m:" + name);
                ++count;
            }
        }
        return count;
    }

    private ICacheService getCacheService() {
        CacheDistributedObject setupRef = instance.getDistributedObject(CacheService.SERVICE_NAME, "setupRef");
        return setupRef.getService();
    }
}
