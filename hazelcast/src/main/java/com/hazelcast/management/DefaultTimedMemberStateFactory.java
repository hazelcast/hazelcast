package com.hazelcast.management;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.impl.CacheDistributedObject;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.memory.MemoryStatsSupport;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.monitor.impl.LocalCacheStatsImpl;
import com.hazelcast.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.monitor.impl.LocalMultiMapStatsImpl;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A Factory for creating {@link com.hazelcast.monitor.TimedMemberState} instances.
 */
public class DefaultTimedMemberStateFactory implements TimedMemberStateFactory {

    private static final ILogger LOGGER = Logger.getLogger(DefaultTimedMemberStateFactory.class);
    private final HazelcastInstanceImpl instance;
    private final int maxVisibleInstanceCount;
    private final boolean cacheServiceEnabled;

    public DefaultTimedMemberStateFactory(HazelcastInstanceImpl instance) {
        this.instance = instance;
        maxVisibleInstanceCount = instance.node.groupProperties.MC_MAX_INSTANCE_COUNT.getInteger();
        cacheServiceEnabled = instance.node.nodeEngine.getService(CacheService.SERVICE_NAME) != null;
    }

    @Override
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
         return MemoryStatsSupport.getMemoryStats();
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
        memberState.clearPartitions();
        for (InternalPartition partition : partitions) {
            Address owner = partition.getOwnerOrNull();
            if (owner != null && thisAddress.equals(owner)) {
                memberState.addPartition(partition.getPartitionId());
            }
        }
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
            if (distributedObject instanceof IMap) {
                count = handleMap(memberState, count, config, (IMap) distributedObject);
            } else if (distributedObject instanceof IQueue) {
                count = handleQueue(memberState, count, config, (IQueue) distributedObject);
            } else if (distributedObject instanceof ITopic) {
                count = handleTopic(memberState, count, config, (ITopic) distributedObject);
            } else if (distributedObject instanceof MultiMap) {
                count = handleMultimap(memberState, count, config, (MultiMap) distributedObject);
            } else if (distributedObject instanceof IExecutorService) {
                count = handleExecutorService(memberState, count, config, (IExecutorService) distributedObject);
            } else {
                LOGGER.finest("Distributed object ignored for monitoring: " + distributedObject.getName());
            }
        }

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

    private int handleMap(MemberStateImpl memberState, int count, Config config, IMap map) {
        if (config.findMapConfig(map.getName()).isStatisticsEnabled()) {
            memberState.putLocalMapStats(map.getName(), (LocalMapStatsImpl) map.getLocalMapStats());
            return count + 1;
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
                } else if (distributedObject instanceof IMap) {
                    count = collectMapName(setLongInstanceNames, count, config, (IMap) distributedObject);
                } else if (distributedObject instanceof IQueue) {
                    count = collectQueueName(setLongInstanceNames, count, config, (IQueue) distributedObject);
                } else if (distributedObject instanceof ITopic) {
                    count = collectTopicName(setLongInstanceNames, count, config, (ITopic) distributedObject);
                } else if (distributedObject instanceof IExecutorService) {
                    count = collectExecutorServiceName(setLongInstanceNames, count, config, (IExecutorService) distributedObject);
                } else {
                    LOGGER.finest("Distributed object ignored for monitoring: " + distributedObject.getName());
                }
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

    private int collectMapName(Set<String> setLongInstanceNames, int count, Config config, IMap map) {
        if (config.findMapConfig(map.getName()).isStatisticsEnabled()) {
            setLongInstanceNames.add("c:" + map.getName());
            return count + 1;
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
}
