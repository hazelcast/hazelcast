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
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.Member;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.monitor.LocalOperationStats;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.LocalReplicatedMapStats;
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
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
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

    private volatile boolean memberStateSafe = true;

    public TimedMemberStateFactory(HazelcastInstanceImpl instance) {
        this.instance = instance;
        Node node = instance.node;
        maxVisibleInstanceCount = node.groupProperties.getInteger(GroupProperty.MC_MAX_VISIBLE_INSTANCE_COUNT);
        cacheServiceEnabled = node.nodeEngine.getService(CacheService.SERVICE_NAME) != null;
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

        TimedMemberState timedMemberState = new TimedMemberState();
        createMemberState(timedMemberState, memberState, services);
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

        return timedMemberState;
    }

    protected LocalMemoryStats getMemoryStats() {
        return new LocalMemoryStatsImpl(instance.getMemoryStats());
    }

    protected LocalOperationStats getOperationStats() {
        return new LocalOperationStatsImpl(instance.node);
    }

    private void createMemberState(TimedMemberState timedMemberState, MemberStateImpl memberState,
                                   Collection<StatisticsAwareService> services) {
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
        createMemState(timedMemberState, memberState, services);
    }

    private void createMemState(TimedMemberState timedMemberState, MemberStateImpl memberState,
                                Collection<StatisticsAwareService> services) {
        int count = 0;
        Config config = instance.getConfig();
        Set<String> longInstanceNames = new HashSet<String>(maxVisibleInstanceCount);
        for (StatisticsAwareService service : services) {
            if (count < maxVisibleInstanceCount) {
                if (service instanceof MapService) {
                    count = handleMap(memberState, count, config, ((MapService) service).getStats(), longInstanceNames);
                } else if (service instanceof MultiMapService) {
                    count = handleMultimap(memberState, count, config, ((MultiMapService) service).getStats(), longInstanceNames);
                } else if (service instanceof QueueService) {
                    count = handleQueue(memberState, count, config, ((QueueService) service).getStats(), longInstanceNames);
                } else if (service instanceof TopicService) {
                    count = handleTopic(memberState, count, config, ((TopicService) service).getStats(), longInstanceNames);
                } else if (service instanceof DistributedExecutorService) {
                    count = handleExecutorService(memberState, count, config,
                            ((DistributedExecutorService) service).getStats(), longInstanceNames);
                } else if (service instanceof ReplicatedMapService) {
                    count = handleReplicatedMap(memberState, count, config, ((ReplicatedMapService) service).getStats(),
                            longInstanceNames);
                }
            }
        }

        if (cacheServiceEnabled) {
            ICacheService cacheService = getCacheService();
            for (CacheConfig cacheConfig : cacheService.getCacheConfigs()) {
                if (cacheConfig.isStatisticsEnabled() && count < maxVisibleInstanceCount) {
                    CacheStatistics statistics = cacheService.getStatistics(cacheConfig.getNameWithPrefix());
                    count = handleCache(memberState, count, cacheConfig, statistics, longInstanceNames);
                }
            }
        }
        timedMemberState.setInstanceNames(longInstanceNames);
    }

    private int handleExecutorService(MemberStateImpl memberState, int count, Config config,
                                      Map<String, LocalExecutorStats> executorServices,
                                      Set<String> longInstanceNames) {

        for (Map.Entry<String, LocalExecutorStats> entry : executorServices.entrySet()) {
            String name = entry.getKey();
            if (count >= maxVisibleInstanceCount) {
                break;
            } else if (config.findExecutorConfig(name).isStatisticsEnabled()) {
                LocalExecutorStats stats = entry.getValue();
                memberState.putLocalExecutorStats(name, stats);
                longInstanceNames.add("e:" + name);
                ++count;
            }
        }
        return count;
    }

    private int handleMultimap(MemberStateImpl memberState, int count, Config config, Map<String, LocalMultiMapStats> multiMaps,
                               Set<String> longInstanceNames) {
        for (Map.Entry<String, LocalMultiMapStats> entry : multiMaps.entrySet()) {
            String name = entry.getKey();
            if (count >= maxVisibleInstanceCount) {
                break;
            } else if (config.findMultiMapConfig(name).isStatisticsEnabled()) {
                LocalMultiMapStats stats = entry.getValue();
                memberState.putLocalMultiMapStats(name, stats);
                longInstanceNames.add("m:" + name);
                ++count;
            }
        }
        return count;
    }

    private int handleReplicatedMap(MemberStateImpl memberState, int count, Config
            config, Map<String, LocalReplicatedMapStats> replicatedMaps, Set<String> longInstanceNames) {
        for (Map.Entry<String, LocalReplicatedMapStats> entry : replicatedMaps.entrySet()) {
            String name = entry.getKey();
            if (count >= maxVisibleInstanceCount) {
                break;
            } else if (config.findReplicatedMapConfig(name).isStatisticsEnabled()) {
                LocalReplicatedMapStats stats = entry.getValue();
                memberState.putLocalReplicatedMapStats(name, stats);
                longInstanceNames.add("r:" + name);
                ++count;
            }
        }
        return count;
    }

    private int handleTopic(MemberStateImpl memberState, int count, Config config, Map<String, LocalTopicStats> topics,
                            Set<String> longInstanceNames) {
        for (Map.Entry<String, LocalTopicStats> entry : topics.entrySet()) {
            String name = entry.getKey();
            if (count >= maxVisibleInstanceCount) {
                break;
            } else if (config.findTopicConfig(name).isStatisticsEnabled()) {
                LocalTopicStats stats = entry.getValue();
                memberState.putLocalTopicStats(name, stats);
                longInstanceNames.add("t:" + name);
                ++count;
            }
        }
        return count;
    }

    private int handleQueue(MemberStateImpl memberState, int count, Config config, Map<String, LocalQueueStats> queues,
                            Set<String> longInstanceNames) {
        for (Map.Entry<String, LocalQueueStats> entry : queues.entrySet()) {
            String name = entry.getKey();
            if (count >= maxVisibleInstanceCount) {
                break;
            } else if (config.findQueueConfig(name).isStatisticsEnabled()) {
                LocalQueueStats stats = entry.getValue();
                memberState.putLocalQueueStats(name, stats);
                longInstanceNames.add("q:" + name);
                ++count;
            }
        }
        return count;
    }

    private int handleMap(MemberStateImpl memberState, int count, Config config, Map<String, LocalMapStats> maps,
                          Set<String> longInstanceNames) {
        for (Map.Entry<String, LocalMapStats> entry : maps.entrySet()) {
            String name = entry.getKey();
            if (count >= maxVisibleInstanceCount) {
                break;
            } else if (config.findMapConfig(name).isStatisticsEnabled()) {
                LocalMapStats stats = entry.getValue();
                memberState.putLocalMapStats(name, stats);
                longInstanceNames.add("c:" + name);
                ++count;
            }
        }
        return count;
    }

    private int handleCache(MemberStateImpl memberState, int count, CacheConfig config, CacheStatistics cacheStatistics,
                            Set<String> longInstanceNames) {
        memberState.putLocalCacheStats(config.getNameWithPrefix(), new LocalCacheStatsImpl(cacheStatistics));
        longInstanceNames.add("j:" + config.getNameWithPrefix());
        return ++count;
    }


    private ICacheService getCacheService() {
        CacheDistributedObject setupRef = instance.getDistributedObject(CacheService.SERVICE_NAME, "setupRef");
        return setupRef.getService();
    }
}
