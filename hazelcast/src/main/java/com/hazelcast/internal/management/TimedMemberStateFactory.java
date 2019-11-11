/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.Client;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.cp.CPMember;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.crdt.pncounter.PNCounterService;
import com.hazelcast.internal.management.dto.AdvancedNetworkStatsDTO;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.monitor.LocalCacheStats;
import com.hazelcast.internal.networking.NetworkStats;
import com.hazelcast.internal.nio.AggregateEndpointManager;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.executor.LocalExecutorStats;
import com.hazelcast.internal.monitor.LocalFlakeIdGeneratorStats;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.internal.monitor.LocalMemoryStats;
import com.hazelcast.multimap.LocalMultiMapStats;
import com.hazelcast.internal.monitor.LocalOperationStats;
import com.hazelcast.internal.monitor.LocalPNCounterStats;
import com.hazelcast.collection.LocalQueueStats;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;
import com.hazelcast.topic.LocalTopicStats;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.internal.monitor.WanSyncState;
import com.hazelcast.internal.monitor.impl.HotRestartStateImpl;
import com.hazelcast.internal.monitor.impl.LocalMemoryStatsImpl;
import com.hazelcast.internal.monitor.impl.LocalOperationStatsImpl;
import com.hazelcast.internal.monitor.impl.MemberPartitionStateImpl;
import com.hazelcast.internal.monitor.impl.MemberStateImpl;
import com.hazelcast.internal.monitor.impl.NodeStateImpl;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.cluster.Address;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;
import com.hazelcast.wan.impl.WanReplicationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

import javax.annotation.Nonnull;

import static com.hazelcast.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * A Factory for creating {@link TimedMemberState} instances.
 */
public class TimedMemberStateFactory {

    private static final int INITIAL_PARTITION_SAFETY_CHECK_DELAY = 15;
    private static final int PARTITION_SAFETY_CHECK_PERIOD = 60;

    protected final HazelcastInstanceImpl instance;

    private volatile boolean memberStateSafe = true;

    public TimedMemberStateFactory(HazelcastInstanceImpl instance) {
        this.instance = instance;

        if (instance.node.getProperties().get("hazelcast.mc.max.visible.instance.count") != null) {
            instance.node.loggingService.getLogger(getClass())
                    .warning("hazelcast.mc.max.visible.instance.count property is removed.");
        }
    }

    public void init() {
        instance.node.nodeEngine.getExecutionService().scheduleWithRepetition(
                () -> memberStateSafe = instance.getPartitionService().isLocalMemberSafe(),
                INITIAL_PARTITION_SAFETY_CHECK_DELAY, PARTITION_SAFETY_CHECK_PERIOD, TimeUnit.SECONDS
        );
    }

    @Nonnull
    public TimedMemberState createTimedMemberState() {
        MemberStateImpl memberState = new MemberStateImpl();
        Collection<StatisticsAwareService> services = instance.node.nodeEngine.getServices(StatisticsAwareService.class);

        TimedMemberState timedMemberState = new TimedMemberState();
        createMemberState(memberState, services);
        timedMemberState.setMaster(instance.node.isMaster());
        timedMemberState.setMemberList(new ArrayList<>());
        Set<Member> memberSet = instance.getCluster().getMembers();
        for (Member member : memberSet) {
            MemberImpl memberImpl = (MemberImpl) member;
            Address address = memberImpl.getAddress();
            timedMemberState.getMemberList().add(address.getHost() + ":" + address.getPort());
        }
        timedMemberState.setMemberState(memberState);
        timedMemberState.setClusterName(instance.getConfig().getClusterName());
        SSLConfig sslConfig = getActiveMemberNetworkConfig(instance.getConfig()).getSSLConfig();
        timedMemberState.setSslEnabled(sslConfig != null && sslConfig.isEnabled());
        timedMemberState.setLite(instance.node.isLiteMember());

        SocketInterceptorConfig interceptorConfig = getActiveMemberNetworkConfig(instance.getConfig())
                .getSocketInterceptorConfig();
        timedMemberState.setSocketInterceptorEnabled(interceptorConfig != null && interceptorConfig.isEnabled());

        ManagementCenterConfig managementCenterConfig = instance.node.getConfig().getManagementCenterConfig();
        timedMemberState.setScriptingEnabled(managementCenterConfig.isScriptingEnabled());

        return timedMemberState;
    }

    protected LocalMemoryStats getMemoryStats() {
        return new LocalMemoryStatsImpl(instance.getMemoryStats());
    }

    private LocalOperationStats getOperationStats() {
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
        memberState.setName(instance.getName());

        memberState.setUuid(node.getThisUuid());
        if (instance.getConfig().getCPSubsystemConfig().getCPMemberCount() == 0) {
            memberState.setCpMemberUuid(null);
        } else {
            CPMember localCPMember = instance.getCPSubsystem().getLocalCPMember();
            memberState.setCpMemberUuid(localCPMember != null ? localCPMember.getUuid() : null);
        }

        Address thisAddress = node.getThisAddress();
        memberState.setAddress(thisAddress.getHost() + ":" + thisAddress.getPort());
        memberState.setEndpoints(node.getLocalMember().getAddressMap());
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

        AggregateEndpointManager aggregateEndpointManager = node.getNetworkingService().getAggregateEndpointManager();
        memberState.setInboundNetworkStats(createAdvancedNetworkStats(aggregateEndpointManager.getNetworkStats(),
                NetworkStats::getBytesReceived));
        memberState.setOutboundNetworkStats(createAdvancedNetworkStats(aggregateEndpointManager.getNetworkStats(),
                NetworkStats::getBytesSent));
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
            } else if (service instanceof CacheService) {
                count = handleCache(memberState, count, (CacheService) service);
            }
        }

        WanReplicationService wanReplicationService = instance.node.nodeEngine.getWanReplicationService();
        Map<String, LocalWanStats> wanStats = wanReplicationService.getStats();
        if (wanStats != null) {
            count = handleWan(memberState, count, wanStats);
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

    private int handleCache(MemberStateImpl memberState, int count, CacheService cacheService) {
        Map<String, LocalCacheStats> map = cacheService.getStats();
        for (Map.Entry<String, LocalCacheStats> entry : map.entrySet()) {
            String name = entry.getKey();
            CacheConfig cacheConfig = cacheService.getCacheConfig(entry.getKey());
            if (cacheConfig != null && cacheConfig.isStatisticsEnabled()) {
                LocalCacheStats stats = entry.getValue();
                memberState.putLocalCacheStats(name, stats);
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

    private AdvancedNetworkStatsDTO createAdvancedNetworkStats(Map<EndpointQualifier, NetworkStats> stats,
                                                               ToLongFunction<NetworkStats> getBytesFn) {
        AdvancedNetworkStatsDTO statsDTO = new AdvancedNetworkStatsDTO();
        for (Map.Entry<EndpointQualifier, NetworkStats> entry : stats.entrySet()) {
            statsDTO.incBytesTransceived(entry.getKey().getType(), getBytesFn.applyAsLong(entry.getValue()));
        }
        return statsDTO;
    }
}
