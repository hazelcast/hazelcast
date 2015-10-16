package com.hazelcast.monitor.impl;


import com.eclipsesource.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalWanPublisherStatsTest {

    @Test
    public void testSerialization() {
        LocalWanPublisherStatsImpl localWanPublisherStats = new LocalWanPublisherStatsImpl();
        localWanPublisherStats.setConnected(true);
        localWanPublisherStats.setOutboundQueueSize(100);
        localWanPublisherStats.incrementPublishedEventCount(10);

        JsonObject serialized = localWanPublisherStats.toJson();

        LocalWanPublisherStatsImpl deserialized = new LocalWanPublisherStatsImpl();
        deserialized.fromJson(serialized);

        assertEquals(localWanPublisherStats.isConnected(), deserialized.isConnected());
        assertEquals(localWanPublisherStats.getTotalPublishedEventCount(), deserialized.getTotalPublishedEventCount());
        assertEquals(localWanPublisherStats.getOutboundQueueSize(), deserialized.getOutboundQueueSize());
        assertEquals(localWanPublisherStats.getTotalPublishLatency(), deserialized.getTotalPublishLatency());
    }

}
