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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.cache.impl.CacheStatisticsImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.management.TimedMemberState;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.monitor.HotRestartState;
import com.hazelcast.internal.monitor.NodeState;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.persistence.BackupTaskState;
import com.hazelcast.persistence.BackupTaskStatus;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
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
import static com.hazelcast.test.Accessors.getHazelcastInstanceImpl;
import static java.util.Collections.singleton;
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
        replicatedMapStats.incrementPutsNanos(30);
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

        ClusterState clusterState = ClusterState.ACTIVE;
        com.hazelcast.instance.impl.NodeState nodeState = com.hazelcast.instance.impl.NodeState.PASSIVE;
        Version clusterVersion = Version.of("3.9.0");
        MemberVersion memberVersion = MemberVersion.of("3.8.0");
        NodeState state = new NodeStateImpl(clusterState, nodeState, clusterVersion, memberVersion);
        final BackupTaskStatus backupTaskStatus = new BackupTaskStatus(BackupTaskState.IN_PROGRESS, 5, 10);
        final String backupDirectory = "/hot/backup/dir";
        final HotRestartStateImpl hotRestartState = new HotRestartStateImpl(backupTaskStatus, true, backupDirectory);

        Map<UUID, String> clientStats = new HashMap<>();
        clientStats.put(clientUuid, "someStats");

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
        memberState.setMapsWithStats(singleton("map-1"));
        memberState.setMultiMapsWithStats(singleton("multiMap-1"));
        memberState.setReplicatedMapsWithStats(singleton("replicatedMap-1"));
        memberState.setQueuesWithStats(singleton("queue-1"));
        memberState.setTopicsWithStats(singleton("topic-1"));
        memberState.setReliableTopicsWithStats(singleton("reliableTopic-1"));
        memberState.setPNCountersWithStats(singleton("pnCounter-1"));
        memberState.setExecutorsWithStats(singleton("executor-1"));
        memberState.setCachesWithStats(singleton("cache-1"));
        memberState.setFlakeIdGeneratorsWithStats(singleton("flakeIdGenerator-1"));
        memberState.setOperationStats(new LocalOperationStatsImpl());
        memberState.setClients(clients);
        memberState.setNodeState(state);
        memberState.setHotRestartState(hotRestartState);
        memberState.setClientStats(clientStats);

        MemberStateImpl deserialized = new MemberStateImpl();
        deserialized.fromJson(memberState.toJson());

        assertEquals("memberStateAddress:Port", deserialized.getAddress());
        assertEquals(uuid, deserialized.getUuid());
        assertEquals(cpMemberUuid, deserialized.getCpMemberUuid());
        assertEquals(endpoints, deserialized.getEndpoints());

        assertNotNull(deserialized.getName());
        assertEquals(deserialized.getName(), memberState.getName());

        assertEquals(singleton("map-1"), deserialized.getMapsWithStats());
        assertEquals(singleton("multiMap-1"), deserialized.getMultiMapsWithStats());
        assertEquals(singleton("replicatedMap-1"), deserialized.getReplicatedMapsWithStats());
        assertEquals(singleton("queue-1"), deserialized.getQueuesWithStats());
        assertEquals(singleton("topic-1"), deserialized.getTopicsWithStats());
        assertEquals(singleton("reliableTopic-1"), deserialized.getReliableTopicsWithStats());
        assertEquals(singleton("pnCounter-1"), deserialized.getPNCountersWithStats());
        assertEquals(singleton("executor-1"), deserialized.getExecutorsWithStats());
        assertEquals(singleton("cache-1"), deserialized.getCachesWithStats());
        assertEquals(singleton("flakeIdGenerator-1"), deserialized.getFlakeIdGeneratorsWithStats());
        assertNotNull(deserialized.getOperationStats());

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

        ClusterHotRestartStatusDTO clusterHotRestartStatus = deserialized.getClusterHotRestartStatus();
        assertEquals(FULL_RECOVERY_ONLY, clusterHotRestartStatus.getDataRecoveryPolicy());
        assertEquals(ClusterHotRestartStatusDTO.ClusterHotRestartStatus.UNKNOWN, clusterHotRestartStatus.getHotRestartStatus());
        assertEquals(-1, clusterHotRestartStatus.getRemainingValidationTimeMillis());
        assertEquals(-1, clusterHotRestartStatus.getRemainingDataLoadTimeMillis());
        assertTrue(clusterHotRestartStatus.getMemberHotRestartStatusMap().isEmpty());

        Map<UUID, String> deserializedClientStats = deserialized.getClientStats();
        assertEquals("someStats", deserializedClientStats.get(clientUuid));
    }
}
