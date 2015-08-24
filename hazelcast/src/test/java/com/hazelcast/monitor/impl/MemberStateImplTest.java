package com.hazelcast.monitor.impl;

import com.hazelcast.cache.impl.CacheStatisticsImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.instance.TestUtil.getHazelcastInstanceImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        CacheStatisticsImpl cacheStatistics = new CacheStatisticsImpl();
        cacheStatistics.increaseCacheHits(5);
        

        Collection<ClientEndPointDTO> clients = new ArrayList<ClientEndPointDTO>();
        ClientEndPointDTO client = new ClientEndPointDTO();
        client.uuid = "abc123456";
        client.address = "localhost";
        client.clientType = "undefined";
        clients.add(client);

        Map<String, Long> runtimeProps = new HashMap<String, Long>();
        runtimeProps.put("prop1", 598123L);

        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hazelcastInstance));
        TimedMemberState timedMemberState = factory.createTimedMemberState();

        MemberStateImpl memberState = timedMemberState.getMemberState();
        memberState.setAddress("memberStateAddress:Port");
        memberState.putLocalMapStats("mapStats", new LocalMapStatsImpl());
        memberState.putLocalMultiMapStats("multiMapStats", new LocalMultiMapStatsImpl());
        memberState.putLocalQueueStats("queueStats", new LocalQueueStatsImpl());
        memberState.putLocalTopicStats("topicStats", new LocalTopicStatsImpl());
        memberState.putLocalExecutorStats("executorStats", new LocalExecutorStatsImpl());
        memberState.putLocalReplicatedMapStats("replicatedMapStats", replicatedMapStats);
        memberState.putLocalCacheStats("cacheStats", new LocalCacheStatsImpl(cacheStatistics));
        memberState.setRuntimeProps(runtimeProps);
        memberState.setLocalMemoryStats(new LocalMemoryStatsImpl());
        memberState.setOperationStats(new LocalOperationStatsImpl());
        memberState.setClients(clients);

        MemberStateImpl deserialized = new MemberStateImpl();
        deserialized.fromJson(memberState.toJson());

        assertEquals("memberStateAddress:Port", deserialized.getAddress());
        assertNotNull(deserialized.getLocalMapStats("mapStats").toString());
        assertNotNull(deserialized.getLocalMultiMapStats("multiMapStats").toString());
        assertNotNull(deserialized.getLocalQueueStats("queueStats").toString());
        assertNotNull(deserialized.getLocalTopicStats("topicStats").toString());
        assertNotNull(deserialized.getLocalExecutorStats("executorStats").toString());
        assertNotNull(deserialized.getLocalReplicatedMapStats("replicatedMapStats").toString());
        assertEquals(1,deserialized.getLocalReplicatedMapStats("replicatedMapStats").getPutOperationCount());
        assertNotNull(deserialized.getLocalCacheStats("cacheStats").toString());
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
    }
}
