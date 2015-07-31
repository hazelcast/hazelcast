package com.hazelcast.monitor.impl;

import com.hazelcast.cache.impl.CacheStatisticsImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.instance.TestUtil.getHazelcastInstanceImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MemberStateImplTest extends HazelcastTestSupport {

    @Test
    public void testDefaultConstructor() {
        MemberStateImpl memberState = new MemberStateImpl();

        assertNotNull(memberState.toString());
    }

    @Test
    public void testSerialization() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();

        CacheStatisticsImpl cacheStatistics = new CacheStatisticsImpl();
        cacheStatistics.increaseCacheHits(5);

        Collection<ClientEndPointDTO> clients = new ArrayList<ClientEndPointDTO>();
        ClientEndPointDTO client = new ClientEndPointDTO();
        client.uuid = "abc123456";
        client.address = "localhost";
        client.clientType = "undefined";
        clients.add(client);

        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hazelcastInstance));
        TimedMemberState timedMemberState = factory.createTimedMemberState();

        MemberStateImpl memberState = timedMemberState.getMemberState();
        memberState.putLocalMapStats("mapStats", new LocalMapStatsImpl());
        memberState.putLocalMultiMapStats("multiMapStats", new LocalMultiMapStatsImpl());
        memberState.putLocalQueueStats("queueStats", new LocalQueueStatsImpl());
        memberState.putLocalTopicStats("topicStats", new LocalTopicStatsImpl());
        memberState.putLocalExecutorStats("executorStats", new LocalExecutorStatsImpl());
        memberState.putLocalCacheStats("cacheStats", new LocalCacheStatsImpl(cacheStatistics));
        memberState.setClients(clients);

        MemberStateImpl deserialized = new MemberStateImpl();
        deserialized.fromJson(memberState.toJson());

        assertEquals(memberState, deserialized);
        assertEquals(memberState.hashCode(), deserialized.hashCode());

        assertNotNull(deserialized.getLocalMapStats("mapStats").toString());
        assertNotNull(deserialized.getLocalMultiMapStats("multiMapStats").toString());
        assertNotNull(deserialized.getLocalQueueStats("queueStats").toString());
        assertNotNull(deserialized.getLocalTopicStats("topicStats").toString());
        assertNotNull(deserialized.getLocalExecutorStats("executorStats").toString());
        assertNotNull(deserialized.getLocalCacheStats("cacheStats").toString());

        assertEquals(5, deserialized.getLocalCacheStats("cacheStats").getCacheHits());

        client = deserialized.getClients().iterator().next();
        assertEquals("abc123456", client.uuid);
        assertEquals("localhost", client.address);
        assertEquals("undefined", client.clientType);
    }
}
