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

package com.hazelcast.internal.management;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.Member;
import com.hazelcast.crdt.pncounter.PNCounterService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.monitor.LocalFlakeIdGeneratorStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.monitor.LocalOperationStats;
import com.hazelcast.monitor.LocalPNCounterStats;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.monitor.impl.HotRestartStateImpl;
import com.hazelcast.monitor.impl.LocalCacheStatsImpl;
import com.hazelcast.monitor.impl.LocalMemoryStatsImpl;
import com.hazelcast.monitor.impl.LocalOperationStatsImpl;
import com.hazelcast.monitor.impl.MemberPartitionStateImpl;
import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.monitor.impl.NodeStateImpl;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.Address;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;
import com.hazelcast.wan.WanReplicationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.SetUtil.createHashSet;

/**
 * A Factory for creating {@link TimedMemberState} instances.
 */
public class TimedMemberStateFactory {

    private static final int INITIAL_PARTITION_SAFETY_CHECK_DELAY = 15;
    private static final int PARTITION_SAFETY_CHECK_PERIOD = 60;

    protected final HazelcastInstanceImpl instance;
    private final boolean cacheServiceEnabled;

    private volatile boolean memberStateSafe = true;

    public TimedMemberStateFactory(HazelcastInstanceImpl instance) {
        this.instance = instance;

        if (instance.node.getProperties().get("hazelcast.mc.max.visible.instance.count") != null) {
            instance.node.loggingService.getLogger(getClass())
                    .warning("hazelcast.mc.max.visible.instance.count property is removed.");
        }
        cacheServiceEnabled = isCacheServiceEnabled();
    }

    private boolean isCacheServiceEnabled() {
        NodeEngineImpl nodeEngine = instance.node.nodeEngine;
        Collection<ServiceInfo> serviceInfos = nodeEngine.getServiceInfos(CacheService.class);
        return !serviceInfos.isEmpty();
    }

    public void init() {
        instance.node.nodeEngine.getExecutionService().scheduleWithRepetition(new Runnable() {
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
        createMemberState(memberState, services);
        timedMemberState.setMaster(instance.node.isMaster());
        timedMemberState.setMemberList(new ArrayList<String>());
        if (timedMemberState.isMaster()) {
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
        SSLConfig sslConfig = instance.getConfig().getNetworkConfig().getSSLConfig();
        timedMemberState.setSslEnabled(sslConfig != null && sslConfig.isEnabled());
        timedMemberState.setLite(instance.node.isLiteMember());

        SocketInterceptorConfig interceptorConfig = instance.getConfig().getNetworkConfig().getSocketInterceptorConfig();
        timedMemberState.setSocketInterceptorEnabled(interceptorConfig != null && interceptorConfig.isEnabled());

        ManagementCenterConfig managementCenterConfig = instance.node.getConfig().getManagementCenterConfig();
        timedMemberState.setScriptingEnabled(managementCenterConfig.isScriptingEnabled());

        return timedMemberState;
    }

    protected LocalMemoryStats getMemoryStats() {
        return new LocalMemoryStatsImpl(instance.getMemoryStats());
    }

    protected LocalOperationStats getOperationStats() {
        return new LocalOperationStatsImpl(instance.node);
    }

    private void createMemberState(MemberStateImpl memberState,
                                   Collection<StatisticsAwareService> services) {
        Node node = instance.node;

        final Collection<Client> clients = instance.node.clientEngine.getClients();
        final Set<ClientEndPointDTO> serializableClientEndPoints = createHashSet(clients.size());
        for (Client client : clients) {
            serializableClientEndPoints.add(new ClientEndPointDTO(client));
        }
        memberState.setClients(serializableClientEndPoints);

        Address thisAddress = node.getThisAddress();
        memberState.setAddress(thisAddress.getHost() + ":" + thisAddress.getPort());
        TimedMemberStateFactoryHelper.registerJMXBeans(instance, memberState);

        MemberPartitionStateImpl memberPartitionState = (MemberPartitionStateImpl) memberState.getMemberPartitionState();
        InternalPartitionService partitionService = node.getPartitionService();
        IPartition[] partitions = partitionService.getPartitions();

        List<Integer> partitionList = memberPartitionState.getPartitions();
        for (IPartition partition : partitions) {
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

        createNodeState(memberState);
        createHotRestartState(memberState);
        createClusterHotRestartStatus(memberState);
        createWanSyncState(memberState);

        memberState.setClientStats(node.clientEngine.getClientStatistics());
    }

    private void createHotRestartState(MemberStateImpl memberState) {
        final HotRestartService hotRestartService = instance.node.getNodeExtension().getHotRestartService();
        boolean hotBackupEnabled = hotRestartService.isHotBackupEnabled();
        String hotBackupDirectory = hotRestartService.getBackupDirectory();
        final HotRestartStateImpl state = new HotRestartStateImpl(hotRestartService.getBackupTaskStatus(),
                hotBackupEnabled, hotBackupDirectory);

        memberState.setHotRestartState(state);
    }

    private void createClusterHotRestartStatus(MemberStateImpl memberState) {
        final ClusterHotRestartStatusDTO state =
                instance.node.getNodeExtension().getInternalHotRestartService().getCurrentClusterHotRestartStatus();
        memberState.setClusterHotRestartStatus(state);
    }

    protected void createNodeState(MemberStateImpl memberState) {
        Node node = instance.node;
        ClusterService cluster = instance.node.clusterService;
        NodeStateImpl nodeState = new NodeStateImpl(cluster.getClusterState(), node.getState(),
                cluster.getClusterVersion(), node.getVersion());
        memberState.setNodeState(nodeState);
    }

    private void createWanSyncState(MemberStateImpl memberState) {
        WanReplicationService wanReplicationService = instance.node.nodeEngine.getWanReplicationService();
        WanSyncState wanSyncState = wanReplicationService.getWanSyncState();
        if (wanSyncState != null) {
            memberState.setWanSyncState(wanSyncState);
        }
    }

    private void createMemState(MemberStateImpl memberState,
                                Collection<StatisticsAwareService> services) {
        int count = 0;
        Config config = instance.getConfig();
        for (StatisticsAwareService service : services) {
            if (service instanceof MapService) {
                count = handleMap(memberState, count, config, ((MapService) service).getStats());
            } else if (service instanceof MultiMapService) {
                count = handleMultimap(memberState, count, config, ((MultiMapService) service).getStats());
            } else if (service instanceof QueueService) {
                count = handleQueue(memberState, count, config, ((QueueService) service).getStats());
            } else if (service instanceof TopicService) {
                count = handleTopic(memberState, count, config, ((TopicService) service).getStats());
            } else if (service instanceof ReliableTopicService) {
                count = handleReliableTopic(memberState, count, config,
                        ((ReliableTopicService) service).getStats());
            } else if (service instanceof DistributedExecutorService) {
                count = handleExecutorService(memberState, count, config,
                        ((DistributedExecutorService) service).getStats());
            } else if (service instanceof ReplicatedMapService) {
                count = handleReplicatedMap(memberState, count, config, ((ReplicatedMapService) service).getStats());
            } else if (service instanceof PNCounterService) {
                count = handlePNCounter(memberState, count, config, ((PNCounterService) service).getStats());
            } else if (service instanceof FlakeIdGeneratorService) {
                count = handleFlakeIdGenerator(memberState, count, config,
                        ((FlakeIdGeneratorService) service).getStats());
            }
        }

        WanReplicationService wanReplicationService = instance.node.nodeEngine.getWanReplicationService();
        Map<String, LocalWanStats> wanStats = wanReplicationService.getStats();
        if (wanStats != null) {
            count = handleWan(memberState, count, wanStats);
        }

        if (cacheServiceEnabled) {
            ICacheService cacheService = getCacheService();
            for (CacheConfig cacheConfig : cacheService.getCacheConfigs()) {
                if (cacheConfig.isStatisticsEnabled()) {
                    CacheStatistics statistics = cacheService.getStatistics(cacheConfig.getNameWithPrefix());
                    //Statistics can be null for a short period of time since config is created at first then stats map
                    //is filled.git
                    if (statistics != null) {
                        count = handleCache(memberState, count, cacheConfig, statistics);
                    }
                }
            }
        }
    }

    private int handleFlakeIdGenerator(MemberStateImpl memberState, int count, Config config,
            Map<String, LocalFlakeIdGeneratorStats> flakeIdstats) {
        for (Map.Entry<String, LocalFlakeIdGeneratorStats> entry : flakeIdstats.entrySet()) {
            String name = entry.getKey();
            if (config.findFlakeIdGeneratorConfig(name).isStatisticsEnabled()) {
                LocalFlakeIdGeneratorStats stats = entry.getValue();
                memberState.putLocalFlakeIdStats(name, stats);
                ++count;
            }
        }
        return count;
    }

    private int handleExecutorService(MemberStateImpl memberState, int count, Config config,
                                      Map<String, LocalExecutorStats> executorServices) {

        for (Map.Entry<String, LocalExecutorStats> entry : executorServices.entrySet()) {
            String name = entry.getKey();
            if (config.findExecutorConfig(name).isStatisticsEnabled()) {
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
            if (config.findMultiMapConfig(name).isStatisticsEnabled()) {
                LocalMultiMapStats stats = entry.getValue();
                memberState.putLocalMultiMapStats(name, stats);
                ++count;
            }
        }
        return count;
    }

    private int handleReplicatedMap(MemberStateImpl memberState, int count, Config
            config, Map<String, LocalReplicatedMapStats> replicatedMaps) {
        for (Map.Entry<String, LocalReplicatedMapStats> entry : replicatedMaps.entrySet()) {
            String name = entry.getKey();
            if (config.findReplicatedMapConfig(name).isStatisticsEnabled()) {
                LocalReplicatedMapStats stats = entry.getValue();
                memberState.putLocalReplicatedMapStats(name, stats);
                ++count;
            }
        }
        return count;
    }

    private int handlePNCounter(MemberStateImpl memberState, int count, Config config,
                                Map<String, LocalPNCounterStats> counters) {
        for (Map.Entry<String, LocalPNCounterStats> entry : counters.entrySet()) {
            String name = entry.getKey();
            if (config.findPNCounterConfig(name).isStatisticsEnabled()) {
                LocalPNCounterStats stats = entry.getValue();
                memberState.putLocalPNCounterStats(name, stats);
                ++count;
            }
        }
        return count;
    }

    private int handleReliableTopic(MemberStateImpl memberState, int count, Config config, Map<String, LocalTopicStats> topics) {
        for (Map.Entry<String, LocalTopicStats> entry : topics.entrySet()) {
            String name = entry.getKey();
            if (config.findReliableTopicConfig(name).isStatisticsEnabled()) {
                LocalTopicStats stats = entry.getValue();
                memberState.putLocalReliableTopicStats(name, stats);
                ++count;
            }
        }
        return count;
    }

    private int handleTopic(MemberStateImpl memberState, int count, Config config, Map<String, LocalTopicStats> topics) {
        for (Map.Entry<String, LocalTopicStats> entry : topics.entrySet()) {
            String name = entry.getKey();
            if (config.findTopicConfig(name).isStatisticsEnabled()) {
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
            if (config.findQueueConfig(name).isStatisticsEnabled()) {
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
            if (config.findMapConfig(name).isStatisticsEnabled()) {
                LocalMapStats stats = entry.getValue();
                memberState.putLocalMapStats(name, stats);
                ++count;
            }
        }
        return count;
    }

    private int handleWan(MemberStateImpl memberState, int count, Map<String, LocalWanStats> wans) {
        for (Map.Entry<String, LocalWanStats> entry : wans.entrySet()) {
            String schemeName = entry.getKey();
            LocalWanStats stats = entry.getValue();
            memberState.putLocalWanStats(schemeName, stats);
            count++;
        }
        return count;
    }

    private int handleCache(MemberStateImpl memberState, int count, CacheConfig config, CacheStatistics cacheStatistics) {
        memberState.putLocalCacheStats(config.getNameWithPrefix(), new LocalCacheStatsImpl(cacheStatistics));
        return ++count;
    }


    private ICacheService getCacheService() {
        return instance.node.nodeEngine.getService(ICacheService.SERVICE_NAME);
    }
}
