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

package com.hazelcast.internal.management;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.Client;
import com.hazelcast.client.impl.statistics.ClientStatistics;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.collection.LocalQueueStats;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.cp.CPMember;
import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.crdt.pncounter.PNCounterService;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.monitor.LocalCacheStats;
import com.hazelcast.internal.monitor.LocalFlakeIdGeneratorStats;
import com.hazelcast.internal.monitor.LocalOperationStats;
import com.hazelcast.internal.monitor.LocalPNCounterStats;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.internal.monitor.impl.HotRestartStateImpl;
import com.hazelcast.internal.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.internal.monitor.impl.LocalOperationStatsImpl;
import com.hazelcast.internal.monitor.impl.MemberPartitionStateImpl;
import com.hazelcast.internal.monitor.impl.MemberStateImpl;
import com.hazelcast.internal.monitor.impl.NodeStateImpl;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.LocalMultiMapStats;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.topic.LocalTopicStats;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;
import com.hazelcast.wan.impl.WanReplicationService;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
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
        timedMemberState.setConsoleEnabled(managementCenterConfig.isConsoleEnabled());
        timedMemberState.setMcDataAccessEnabled(managementCenterConfig.isDataAccessEnabled());

        return timedMemberState;
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

        MemberPartitionStateImpl memberPartitionState = (MemberPartitionStateImpl) memberState.getMemberPartitionState();
        InternalPartitionService partitionService = node.getPartitionService();
        IPartition[] partitions = partitionService.getPartitions();

        List<Integer> partitionList = memberPartitionState.getPartitions();
        for (IPartition partition : partitions) {
            if (partition.isLocal()) {
                partitionList.add(partition.getPartitionId());
            }
        }
        memberPartitionState.setMemberStateSafe(memberStateSafe);

        memberState.setOperationStats(getOperationStats());
        createMemState(memberState, services);

        createNodeState(memberState);
        createHotRestartState(memberState);
        createClusterHotRestartStatus(memberState);

        memberState.setClientStats(getClientAttributes(node.getClientEngine().getClientStatistics()));
    }

    private Map<UUID, String> getClientAttributes(Map<UUID, ClientStatistics> allClientStatistics) {
        Map<UUID, String> statsMap = createHashMap(allClientStatistics.size());
        for (Map.Entry<UUID, ClientStatistics> entry : allClientStatistics.entrySet()) {
            UUID uuid = entry.getKey();
            ClientStatistics statistics = entry.getValue();
            if (statistics != null) {
                statsMap.put(uuid, statistics.clientAttributes());
            }
        }
        return statsMap;
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

    private void createMemState(MemberStateImpl memberState,
                                Collection<StatisticsAwareService> services) {
        Config config = instance.getConfig();
        for (StatisticsAwareService service : services) {
            if (service instanceof MapService) {
                handleMap(memberState, ((MapService) service).getStats());
            } else if (service instanceof MultiMapService) {
                handleMultiMap(memberState, ((MultiMapService) service).getStats());
            } else if (service instanceof QueueService) {
                handleQueue(memberState, ((QueueService) service).getStats());
            } else if (service instanceof TopicService) {
                handleTopic(memberState, ((TopicService) service).getStats());
            } else if (service instanceof ReliableTopicService) {
                handleReliableTopic(memberState, ((ReliableTopicService) service).getStats());
            } else if (service instanceof DistributedExecutorService) {
                handleExecutorService(memberState, config, ((DistributedExecutorService) service).getStats());
            } else if (service instanceof DistributedScheduledExecutorService) {
                handleScheduledExecutorService(memberState, config,
                        ((DistributedScheduledExecutorService) service).getStats());
            } else if (service instanceof DistributedDurableExecutorService) {
                handleDurableExecutorService(memberState, config, ((DistributedDurableExecutorService) service).getStats());
            } else if (service instanceof ReplicatedMapService) {
                handleReplicatedMap(memberState, config, ((ReplicatedMapService) service).getStats());
            } else if (service instanceof PNCounterService) {
                handlePNCounter(memberState, config, ((PNCounterService) service).getStats());
            } else if (service instanceof FlakeIdGeneratorService) {
                handleFlakeIdGenerator(memberState, config, ((FlakeIdGeneratorService) service).getStats());
            } else if (service instanceof CacheService) {
                handleCache(memberState, (CacheService) service);
            }
        }

        WanReplicationService wanReplicationService = instance.node.nodeEngine.getWanReplicationService();
        Map<String, LocalWanStats> wanStats = wanReplicationService.getStats();
        if (wanStats != null) {
            handleWan(memberState, wanStats);
        }
    }

    private void handleFlakeIdGenerator(MemberStateImpl memberState, Config config,
                                        Map<String, LocalFlakeIdGeneratorStats> flakeIdStats) {
        Set<String> flakeIdGeneratorsWithStats = createHashSet(flakeIdStats.size());
        for (String name : flakeIdStats.keySet()) {
            if (config.findFlakeIdGeneratorConfig(name).isStatisticsEnabled()) {
                flakeIdGeneratorsWithStats.add(name);
            }
        }
        memberState.setFlakeIdGeneratorsWithStats(flakeIdGeneratorsWithStats);
    }

    private void handleExecutorService(MemberStateImpl memberState, Config config,
                                       Map<String, LocalExecutorStatsImpl> executorServices) {
        Set<String> executorsWithStats = createHashSet(executorServices.size());
        for (String name : executorServices.keySet()) {
            if (config.findExecutorConfig(name).isStatisticsEnabled()) {
                executorsWithStats.add(name);
            }
        }
        memberState.setExecutorsWithStats(executorsWithStats);
    }

    private void handleScheduledExecutorService(MemberStateImpl memberState,
                                                Config config,
                                                Map<String, LocalExecutorStatsImpl> executorServices) {
        Set<String> executorsWithStats = createHashSet(executorServices.size());
        for (String name : executorServices.keySet()) {
            if (config.findScheduledExecutorConfig(name).isStatisticsEnabled()) {
                executorsWithStats.add(name);
            }
        }
        memberState.setScheduledExecutorsWithStats(executorsWithStats);
    }

    private void handleDurableExecutorService(MemberStateImpl memberState,
                                              Config config,
                                              Map<String, LocalExecutorStatsImpl> executorServices) {
        Set<String> executorsWithStats = createHashSet(executorServices.size());
        for (String name : executorServices.keySet()) {
            if (config.findDurableExecutorConfig(name).isStatisticsEnabled()) {
                executorsWithStats.add(name);
            }
        }
        memberState.setDurableExecutorsWithStats(executorsWithStats);
    }

    private void handleMultiMap(MemberStateImpl memberState,
                                Map<String, LocalMultiMapStats> multiMaps) {
        memberState.setMultiMapsWithStats(multiMaps.keySet());
    }

    private void handleReplicatedMap(MemberStateImpl memberState, Config config,
                                     Map<String, LocalReplicatedMapStats> replicatedMaps) {
        Set<String> mapsWithStats = createHashSet(replicatedMaps.size());
        for (String name : replicatedMaps.keySet()) {
            if (config.findReplicatedMapConfig(name).isStatisticsEnabled()) {
                mapsWithStats.add(name);
            }
        }
        memberState.setReplicatedMapsWithStats(mapsWithStats);
    }

    private void handlePNCounter(MemberStateImpl memberState, Config config,
                                 Map<String, LocalPNCounterStats> counters) {
        Set<String> countersWithStats = createHashSet(counters.size());
        for (String name : counters.keySet()) {
            if (config.findPNCounterConfig(name).isStatisticsEnabled()) {
                countersWithStats.add(name);
            }
        }
        memberState.setPNCountersWithStats(countersWithStats);
    }

    private void handleReliableTopic(MemberStateImpl memberState,
                                     Map<String, LocalTopicStats> topics) {
        memberState.setReliableTopicsWithStats(topics.keySet());
    }

    private void handleTopic(MemberStateImpl memberState, Map<String, LocalTopicStats> topics) {
        memberState.setTopicsWithStats(topics.keySet());
    }

    private void handleQueue(MemberStateImpl memberState, Map<String, LocalQueueStats> queues) {
        memberState.setQueuesWithStats(queues.keySet());
    }

    private void handleMap(MemberStateImpl memberState, Map<String, LocalMapStats> maps) {
        memberState.setMapsWithStats(maps.keySet());
    }

    private void handleCache(MemberStateImpl memberState, CacheService cacheService) {
        Map<String, LocalCacheStats> map = cacheService.getStats();
        Set<String> cachesWithStats = createHashSet(map.size());
        for (String name : map.keySet()) {
            CacheConfig cacheConfig = cacheService.getCacheConfig(name);
            if (cacheConfig != null && cacheConfig.isStatisticsEnabled()) {
                cachesWithStats.add(name);
            }
        }
        memberState.setCachesWithStats(cachesWithStats);
    }

    private void handleWan(MemberStateImpl memberState, Map<String, LocalWanStats> wans) {
        for (Map.Entry<String, LocalWanStats> entry : wans.entrySet()) {
            String schemeName = entry.getKey();
            LocalWanStats stats = entry.getValue();
            memberState.putLocalWanStats(schemeName, stats);
        }
    }
}
