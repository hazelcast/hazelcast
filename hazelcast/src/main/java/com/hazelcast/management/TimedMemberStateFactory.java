/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.impl.CacheDistributedObject;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.monitor.impl.LocalCacheStatsImpl;
import com.hazelcast.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.monitor.impl.LocalMemoryStatsImpl;
import com.hazelcast.monitor.impl.LocalMultiMapStatsImpl;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.monitor.impl.MemberPartitionStateImpl;
import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.util.MapUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
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

    private final ILogger logger;
    private final HazelcastInstanceImpl instance;
    private final int maxVisibleInstanceCount;
    private final boolean cacheServiceEnabled;
    private volatile boolean memberStateSafe = true;

    public TimedMemberStateFactory(final HazelcastInstanceImpl instance) {
        this.instance = instance;
        maxVisibleInstanceCount = instance.node.groupProperties.MC_MAX_INSTANCE_COUNT.getInteger();
        cacheServiceEnabled = instance.node.nodeEngine.getService(CacheService.SERVICE_NAME) != null;
        logger = instance.node.getLogger(TimedMemberStateFactory.class);
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
        createMemberState(memberState);
        GroupConfig groupConfig = instance.getConfig().getGroupConfig();
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
        timedMemberState.setClusterName(groupConfig.getName());
        timedMemberState.setInstanceNames(getLongInstanceNames());
        return timedMemberState;
    }

    protected LocalMemoryStats getMemoryStats() {
        return new LocalMemoryStatsImpl(instance.getMemoryStats());
    }

    private void createMemberState(MemberStateImpl memberState) {
        final Node node = instance.node;
        Address thisAddress = node.getThisAddress();
        InternalPartitionService partitionService = node.getPartitionService();
        InternalPartition[] partitions = partitionService.getPartitions();
        final HashSet<SerializableClientEndPoint> serializableClientEndPoints = new HashSet<SerializableClientEndPoint>();
        for (Client client : instance.node.clientEngine.getClients()) {
            serializableClientEndPoints.add(new SerializableClientEndPoint(client));
        }
        memberState.setClients(serializableClientEndPoints);
        memberState.setAddress(thisAddress.getHost() + ":" + thisAddress.getPort());
        TimedMemberStateFactoryHelper.registerJMXBeans(instance, memberState);
        MemberPartitionStateImpl memberPartitionState = (MemberPartitionStateImpl) memberState.getMemberPartitionState();
        List<Integer> partitionList = memberPartitionState.getPartitions();
        for (InternalPartition partition : partitions) {
            Address owner = partition.getOwnerOrNull();
            if (owner != null && thisAddress.equals(owner)) {
                partitionList.add(partition.getPartitionId());
            }
        }
        memberPartitionState.setMigrationQueueSize(partitionService.getMigrationQueueSize());
        memberPartitionState.setMemberStateSafe(memberStateSafe);

        memberState.setLocalMemoryStats(getMemoryStats());
        Collection<DistributedObject> proxyObjects = new ArrayList<DistributedObject>(instance.getDistributedObjects());
        TimedMemberStateFactoryHelper.createRuntimeProps(memberState);
        createMemState(memberState, proxyObjects);
    }

    private void createMemState(MemberStateImpl memberState,
                                Collection<DistributedObject> distributedObjects) {
        int count = 0;
        final Config config = instance.getConfig();
        final Iterator<DistributedObject> iterator = distributedObjects.iterator();

        while (iterator.hasNext() && count < maxVisibleInstanceCount) {
            DistributedObject distributedObject = iterator.next();
            if (distributedObject instanceof IQueue) {
                count = handleQueue(memberState, count, config, (IQueue) distributedObject);
            } else if (distributedObject instanceof ITopic) {
                count = handleTopic(memberState, count, config, (ITopic) distributedObject);
            } else if (distributedObject instanceof MultiMap) {
                count = handleMultimap(memberState, count, config, (MultiMap) distributedObject);
            } else if (distributedObject instanceof IExecutorService) {
                count = handleExecutorService(memberState, count, config, (IExecutorService) distributedObject);
            } else {
                logger.finest("Distributed object ignored for monitoring: " + distributedObject.getName());
            }
        }

        /*
        Collect map statistics from map service, backport for
        https://github.com/hazelcast/management-center/issues/153
        */
        count = handleMap(memberState, count, getMapStats());

        if (cacheServiceEnabled) {
            final ICacheService cacheService = getCacheService();
            for (CacheConfig cacheConfig : cacheService.getCacheConfigs()) {
                if (cacheConfig.isStatisticsEnabled()) {
                    CacheStatistics statistics = cacheService.getStatistics(cacheConfig.getNameWithPrefix());
                    count = handleCache(memberState, count, cacheConfig, statistics);
                }
            }
        }
    }

    private int handleExecutorService(MemberStateImpl memberState, int count, Config config, IExecutorService executorService) {
        if (config.findExecutorConfig(executorService.getName()).isStatisticsEnabled()) {
            LocalExecutorStatsImpl stats = (LocalExecutorStatsImpl) executorService.getLocalExecutorStats();
            memberState.putLocalExecutorStats(executorService.getName(), stats);
            return count + 1;
        }
        return count;
    }

    private int handleMultimap(MemberStateImpl memberState, int count, Config config, MultiMap multiMap) {
        if (config.findMultiMapConfig(multiMap.getName()).isStatisticsEnabled()) {
            LocalMultiMapStatsImpl stats = (LocalMultiMapStatsImpl) multiMap.getLocalMultiMapStats();
            memberState.putLocalMultiMapStats(multiMap.getName(), stats);
            return count + 1;
        }
        return count;
    }

    private int handleTopic(MemberStateImpl memberState, int count, Config config, ITopic topic) {
        if (config.findTopicConfig(topic.getName()).isStatisticsEnabled()) {
            LocalTopicStatsImpl stats = (LocalTopicStatsImpl) topic.getLocalTopicStats();
            memberState.putLocalTopicStats(topic.getName(), stats);
            return count + 1;
        }
        return count;
    }

    private int handleQueue(MemberStateImpl memberState, int count, Config config, IQueue queue) {
        if (config.findQueueConfig(queue.getName()).isStatisticsEnabled()) {
            LocalQueueStatsImpl stats = (LocalQueueStatsImpl) queue.getLocalQueueStats();
            memberState.putLocalQueueStats(queue.getName(), stats);
            return count + 1;
        }
        return count;
    }

    private int handleMap(MemberStateImpl memberState, int count, Map<String, LocalMapStatsImpl> maps) {
        for (Map.Entry<String, LocalMapStatsImpl> entry : maps.entrySet()) {
            if (count >= maxVisibleInstanceCount) {
                break;
            } else {
                memberState.putLocalMapStats(entry.getKey(), entry.getValue());
                count = count + 1;
            }
        }
        return count;
    }

    private int handleCache(MemberStateImpl memberState, int count, CacheConfig config, CacheStatistics cacheStatistics) {
        memberState.putLocalCacheStats(config.getNameWithPrefix(), new LocalCacheStatsImpl(cacheStatistics));
        return count + 1;
    }

    private Set<String> getLongInstanceNames() {
        Set<String> setLongInstanceNames = new HashSet<String>(maxVisibleInstanceCount);
        Collection<DistributedObject> proxyObjects = new ArrayList<DistributedObject>(instance.getDistributedObjects());
        collectInstanceNames(setLongInstanceNames, proxyObjects);
        return setLongInstanceNames;
    }

    private void collectInstanceNames(Set<String> setLongInstanceNames,
                                      Collection<DistributedObject> distributedObjects) {
        int count = 0;
        final Config config = instance.getConfig();

        for (DistributedObject distributedObject : distributedObjects) {
            if (count < maxVisibleInstanceCount) {
                if (distributedObject instanceof MultiMap) {
                    count = collectMultiMapName(setLongInstanceNames, count, config, (MultiMap) distributedObject);
                } else if (distributedObject instanceof IQueue) {
                    count = collectQueueName(setLongInstanceNames, count, config, (IQueue) distributedObject);
                } else if (distributedObject instanceof ITopic) {
                    count = collectTopicName(setLongInstanceNames, count, config, (ITopic) distributedObject);
                } else if (distributedObject instanceof IExecutorService) {
                    count = collectExecutorServiceName(setLongInstanceNames, count, config, (IExecutorService) distributedObject);
                } else {
                    logger.finest("Distributed object ignored for monitoring: " + distributedObject.getName());
                }
            }
        }

        /*Collect IMap instance names from map service, backport for
        https://github.com/hazelcast/management-center/issues/153
        */
        count = collectMapName(setLongInstanceNames, count, config, getMapStats().keySet());

        if (cacheServiceEnabled) {
            for (CacheConfig cacheConfig : getCacheService().getCacheConfigs()) {
                if (cacheConfig.isStatisticsEnabled()) {
                    count = collectCacheName(setLongInstanceNames, count, cacheConfig);
                }
            }
        }

    }

    private int collectExecutorServiceName(Set<String> setLongInstanceNames, int count, Config config,
                                           IExecutorService executorService) {
        if (config.findExecutorConfig(executorService.getName()).isStatisticsEnabled()) {
            setLongInstanceNames.add("e:" + executorService.getName());
            return count + 1;
        }
        return count;

    }

    private int collectTopicName(Set<String> setLongInstanceNames, int count, Config config, ITopic topic) {
        if (config.findTopicConfig(topic.getName()).isStatisticsEnabled()) {
            setLongInstanceNames.add("t:" + topic.getName());
            return count + 1;
        }
        return count;
    }

    private int collectQueueName(Set<String> setLongInstanceNames, int count, Config config, IQueue queue) {
        if (config.findQueueConfig(queue.getName()).isStatisticsEnabled()) {
            setLongInstanceNames.add("q:" + queue.getName());
            return count + 1;
        }
        return count;
    }

    private int collectMapName(Set<String> setLongInstanceNames, int count, Config config, Set<String> mapNames) {
        for (String name : mapNames) {
            if (count < maxVisibleInstanceCount) {
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

    private int collectMultiMapName(Set<String> setLongInstanceNames, int count, Config config, MultiMap multiMap) {
        if (config.findMultiMapConfig(multiMap.getName()).isStatisticsEnabled()) {
            setLongInstanceNames.add("m:" + multiMap.getName());
            return count + 1;
        }
        return count;
    }

    private ICacheService getCacheService() {
        final CacheDistributedObject setupRef = instance.getDistributedObject(CacheService.SERVICE_NAME, "setupRef");
        return setupRef.getService();
    }

    private MapService getMapService() {
        return instance.node.nodeEngine.getService(MapService.SERVICE_NAME);
    }

    /**
     * Backport: https://github.com/hazelcast/hazelcast/pull/4413
     * This method will not take place in 3.5 release. All statistics will be provided by
     * <a href="https://github.com/hazelcast/hazelcast/blob/master/hazelcast/src/main/java/com/
     * hazelcast/spi/StatisticsAwareService.java">StatisticsAwareService</a> implementations
     */
    private Map<String, LocalMapStatsImpl> getMapStats() {
        MapServiceContext msc = getMapService().getMapServiceContext();
        Map<String, MapContainer> mapContainers = msc.getMapContainers();
        Map<String, LocalMapStatsImpl> mapStats = MapUtil.createHashMap(mapContainers.size());
        for (Map.Entry<String, MapContainer> entry : mapContainers.entrySet()) {
            String mapName = entry.getKey();
            MapConfig mapConfig = entry.getValue().getMapConfig();
            if (mapConfig.isStatisticsEnabled()) {
                mapStats.put(mapName, msc.getLocalMapStatsProvider().createLocalMapStats(mapName));
            }
        }
        return mapStats;
    }
}
