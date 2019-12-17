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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.cache.impl.CacheStatisticsImpl;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.hotrestart.BackupTaskStatus;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.management.TimedMemberState;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.internal.management.dto.AdvancedNetworkStatsDTO;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.monitor.HotRestartState;
import com.hazelcast.internal.monitor.NodeState;
import com.hazelcast.internal.monitor.WanSyncState;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import com.hazelcast.wan.impl.WanSyncStatus;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberStateImplTest extends HazelcastTestSupport {

    @Test
    public void testDefaultConstructor() {
        MemberStateImpl memberState = new MemberStateImpl();

        assertNotNull(memberState.toString());
    }

    @Test
    public void testSerialization()
            throws UnknownHostException {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();

        LocalReplicatedMapStatsImpl replicatedMapStats = new LocalReplicatedMapStatsImpl();
        replicatedMapStats.incrementPuts(30);
        CacheStatisticsImpl cacheStatistics = new CacheStatisticsImpl(Clock.currentTimeMillis());
        cacheStatistics.increaseCacheHits(5);
        UUID clientUuid = UUID.randomUUID();

        Collection<ClientEndPointDTO> clients = new ArrayList<>();
        ClientEndPointDTO client = new ClientEndPointDTO();
        client.uuid = clientUuid;
        client.address = "localhost";
        client.clientType = "undefined";
        client.name = "aClient";
        client.labels = new HashSet<>(Collections.singletonList("label"));
        client.ipAddress = "10.176.167.34";
        client.canonicalHostName = "ip-10-176-167-34.ec2.internal";
        clients.add(client);

        Map<String, Long> runtimeProps = new HashMap<>();
        runtimeProps.put("prop1", 598123L);

        ClusterState clusterState = ClusterState.ACTIVE;
        com.hazelcast.instance.impl.NodeState nodeState = com.hazelcast.instance.impl.NodeState.PASSIVE;
        Version clusterVersion = Version.of("3.9.0");
        MemberVersion memberVersion = MemberVersion.of("3.8.0");
        NodeState state = new NodeStateImpl(clusterState, nodeState, clusterVersion, memberVersion);
        final BackupTaskStatus backupTaskStatus = new BackupTaskStatus(BackupTaskState.IN_PROGRESS, 5, 10);
        final String backupDirectory = "/hot/backup/dir";
        final HotRestartStateImpl hotRestartState = new HotRestartStateImpl(backupTaskStatus, true, backupDirectory);
        final WanSyncState wanSyncState = new WanSyncStateImpl(WanSyncStatus.IN_PROGRESS, 86, "atob", "B");

        Map<UUID, String> clientStats = new HashMap<>();
        clientStats.put(clientUuid, "someStats");

        AdvancedNetworkStatsDTO inboundNetworkStats = new AdvancedNetworkStatsDTO();
        inboundNetworkStats.incBytesTransceived(ProtocolType.MEMBER, 42);
        AdvancedNetworkStatsDTO outboundNetworkStats = new AdvancedNetworkStatsDTO();
        outboundNetworkStats.incBytesTransceived(ProtocolType.MEMBER, 24);

        Map<EndpointQualifier, Address> endpoints = new HashMap<>();
        endpoints.put(EndpointQualifier.MEMBER, new Address("127.0.0.1", 5701));
        endpoints.put(EndpointQualifier.resolve(ProtocolType.WAN, "MyWAN"), new Address("127.0.0.1", 5901));

        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hazelcastInstance));
        TimedMemberState timedMemberState = factory.createTimedMemberState();

        MemberStateImpl memberState = timedMemberState.getMemberState();
        memberState.setAddress("memberStateAddress:Port");
        UUID uuid = UUID.randomUUID();
        memberState.setUuid(uuid);
        UUID cpMemberUuid = UUID.randomUUID();
        memberState.setCpMemberUuid(cpMemberUuid);
        memberState.setEndpoints(endpoints);
        memberState.putLocalMapStats("mapStats", new LocalMapStatsImpl());
        memberState.putLocalMultiMapStats("multiMapStats", new LocalMultiMapStatsImpl());
        memberState.putLocalQueueStats("queueStats", new LocalQueueStatsImpl());
        memberState.putLocalTopicStats("topicStats", new LocalTopicStatsImpl());
        memberState.putLocalReliableTopicStats("reliableTopicStats", new LocalTopicStatsImpl());
        memberState.putLocalPNCounterStats("pnCounterStats", new LocalPNCounterStatsImpl());
        memberState.putLocalExecutorStats("executorStats", new LocalExecutorStatsImpl());
        memberState.putLocalReplicatedMapStats("replicatedMapStats", replicatedMapStats);
        memberState.putLocalCacheStats("cacheStats", new LocalCacheStatsImpl(cacheStatistics));
        memberState.putLocalFlakeIdStats("flakeIdStats", new LocalFlakeIdGeneratorStatsImpl());
        memberState.setRuntimeProps(runtimeProps);
        memberState.setLocalMemoryStats(new LocalMemoryStatsImpl());
        memberState.setOperationStats(new LocalOperationStatsImpl());
        memberState.setClients(clients);
        memberState.setNodeState(state);
        memberState.setHotRestartState(hotRestartState);
        memberState.setWanSyncState(wanSyncState);
        memberState.setClientStats(clientStats);
        memberState.setInboundNetworkStats(inboundNetworkStats);
        memberState.setOutboundNetworkStats(outboundNetworkStats);

        MemberStateImpl deserialized = new MemberStateImpl();
        deserialized.fromJson(memberState.toJson());

        assertEquals("memberStateAddress:Port", deserialized.getAddress());
        assertEquals(uuid, deserialized.getUuid());
        assertEquals(cpMemberUuid, deserialized.getCpMemberUuid());
        assertEquals(endpoints, deserialized.getEndpoints());

        assertNotNull(deserialized.getName());
        assertEquals(deserialized.getName(), memberState.getName());

        assertNotNull(deserialized.getLocalMapStats("mapStats").toString());
        assertNotNull(deserialized.getLocalMultiMapStats("multiMapStats").toString());
        assertNotNull(deserialized.getLocalQueueStats("queueStats").toString());
        assertNotNull(deserialized.getLocalTopicStats("topicStats").toString());
        assertNotNull(deserialized.getReliableLocalTopicStats("reliableTopicStats").toString());
        assertNotNull(deserialized.getLocalPNCounterStats("pnCounterStats").toString());
        assertNotNull(deserialized.getLocalExecutorStats("executorStats").toString());
        assertNotNull(deserialized.getLocalReplicatedMapStats("replicatedMapStats").toString());
        assertEquals(1, deserialized.getLocalReplicatedMapStats("replicatedMapStats").getPutOperationCount());
        assertNotNull(deserialized.getLocalCacheStats("cacheStats").toString());
        assertNotNull(deserialized.getLocalFlakeIdGeneratorStats("flakeIdStats").toString());
        assertEquals(5, deserialized.getLocalCacheStats("cacheStats").getCacheHits());
        assertNotNull(deserialized.getRuntimeProps());
        assertEquals(Long.valueOf(598123L), deserialized.getRuntimeProps().get("prop1"));
        assertNotNull(deserialized.getLocalMemoryStats());
        assertNotNull(deserialized.getOperationStats());
        assertNotNull(deserialized.getMXBeans());

        client = deserialized.getClients().iterator().next();
        assertEquals(clientUuid, client.uuid);
        assertEquals("localhost", client.address);
        assertEquals("undefined", client.clientType);
        assertEquals("aClient", client.name);
        assertContains(client.labels, "label");
        assertEquals("10.176.167.34", client.ipAddress);
        assertEquals("ip-10-176-167-34.ec2.internal", client.canonicalHostName);

        NodeState deserializedState = deserialized.getNodeState();
        assertEquals(clusterState, deserializedState.getClusterState());
        assertEquals(nodeState, deserializedState.getNodeState());
        assertEquals(clusterVersion, deserializedState.getClusterVersion());
        assertEquals(memberVersion, deserializedState.getMemberVersion());

        final HotRestartState deserializedHotRestartState = deserialized.getHotRestartState();
        assertTrue(deserializedHotRestartState.isHotBackupEnabled());
        assertEquals(backupTaskStatus, deserializedHotRestartState.getBackupTaskStatus());
        assertEquals(backupDirectory, deserializedHotRestartState.getBackupDirectory());

        final WanSyncState deserializedWanSyncState = deserialized.getWanSyncState();
        assertEquals(WanSyncStatus.IN_PROGRESS, deserializedWanSyncState.getStatus());
        assertEquals(86, deserializedWanSyncState.getSyncedPartitionCount());
        assertEquals("atob", deserializedWanSyncState.getActiveWanConfigName());
        assertEquals("B", deserializedWanSyncState.getActivePublisherName());

        ClusterHotRestartStatusDTO clusterHotRestartStatus = deserialized.getClusterHotRestartStatus();
        assertEquals(FULL_RECOVERY_ONLY, clusterHotRestartStatus.getDataRecoveryPolicy());
        assertEquals(ClusterHotRestartStatusDTO.ClusterHotRestartStatus.UNKNOWN, clusterHotRestartStatus.getHotRestartStatus());
        assertEquals(-1, clusterHotRestartStatus.getRemainingValidationTimeMillis());
        assertEquals(-1, clusterHotRestartStatus.getRemainingDataLoadTimeMillis());
        assertTrue(clusterHotRestartStatus.getMemberHotRestartStatusMap().isEmpty());

        Map<UUID, String> deserializedClientStats = deserialized.getClientStats();
        assertEquals("someStats", deserializedClientStats.get(clientUuid));

        assertNotNull(deserialized.getInboundNetworkStats());
        assertEquals(42, deserialized.getInboundNetworkStats().getBytesTransceived(ProtocolType.MEMBER));
        assertNotNull(deserialized.getOutboundNetworkStats());
        assertEquals(24, deserialized.getOutboundNetworkStats().getBytesTransceived(ProtocolType.MEMBER));
    }
}
