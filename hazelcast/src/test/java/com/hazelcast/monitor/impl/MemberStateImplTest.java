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

package com.hazelcast.monitor.impl;

import com.hazelcast.cache.impl.CacheStatisticsImpl;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.hotrestart.BackupTaskStatus;
import com.hazelcast.internal.management.TimedMemberState;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.monitor.HotRestartState;
import com.hazelcast.monitor.NodeState;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanSyncStatus;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberStateImplTest extends HazelcastTestSupport {

    @Test
    public void testDefaultConstructor() {
        MemberStateImpl memberState = new MemberStateImpl();

        assertNotNull(memberState.toString());
    }

    @Test
    public void testSerialization() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();

        LocalReplicatedMapStatsImpl replicatedMapStats = new LocalReplicatedMapStatsImpl();
        replicatedMapStats.incrementPuts(30);
        CacheStatisticsImpl cacheStatistics = new CacheStatisticsImpl(Clock.currentTimeMillis());
        cacheStatistics.increaseCacheHits(5);

        Collection<ClientEndPointDTO> clients = new ArrayList<ClientEndPointDTO>();
        ClientEndPointDTO client = new ClientEndPointDTO();
        client.uuid = "abc123456";
        client.address = "localhost";
        client.clientType = "undefined";
        client.name = "aClient";
        client.attributes = Collections.singletonMap("attrKey", "attrValue");
        clients.add(client);

        Map<String, Long> runtimeProps = new HashMap<String, Long>();
        runtimeProps.put("prop1", 598123L);

        ClusterState clusterState = ClusterState.ACTIVE;
        com.hazelcast.instance.NodeState nodeState = com.hazelcast.instance.NodeState.PASSIVE;
        Version clusterVersion = Version.of("3.9.0");
        MemberVersion memberVersion = MemberVersion.of("3.8.0");
        NodeState state = new NodeStateImpl(clusterState, nodeState, clusterVersion, memberVersion);
        final BackupTaskStatus backupTaskStatus = new BackupTaskStatus(BackupTaskState.IN_PROGRESS, 5, 10);
        final String backupDirectory = "/hot/backup/dir";
        final HotRestartStateImpl hotRestartState = new HotRestartStateImpl(backupTaskStatus, true, backupDirectory);
        final WanSyncState wanSyncState = new WanSyncStateImpl(WanSyncStatus.IN_PROGRESS, 86, "atob", "B");

        Map<String, String> clientStats = new HashMap<String, String>();
        clientStats.put("abc123456", "someStats");

        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hazelcastInstance));
        TimedMemberState timedMemberState = factory.createTimedMemberState();

        MemberStateImpl memberState = timedMemberState.getMemberState();
        memberState.setAddress("memberStateAddress:Port");
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

        MemberStateImpl deserialized = new MemberStateImpl();
        deserialized.fromJson(memberState.toJson());

        assertEquals("memberStateAddress:Port", deserialized.getAddress());
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
        assertEquals("abc123456", client.uuid);
        assertEquals("localhost", client.address);
        assertEquals("undefined", client.clientType);
        assertEquals("aClient", client.name);
        assertEquals("attrValue", client.attributes.get("attrKey"));

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

        Map<String, String> deserializedClientStats = deserialized.getClientStats();
        assertEquals("someStats", deserializedClientStats.get("abc123456"));
    }
}
