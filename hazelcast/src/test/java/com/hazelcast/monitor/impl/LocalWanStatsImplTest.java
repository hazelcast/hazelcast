package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalWanStatsImplTest {

    @Test
    public void testSerialization() {
        LocalWanPublisherStatsImpl tokyo = new LocalWanPublisherStatsImpl();
        tokyo.setConnected(true);
        tokyo.incrementPublishedEventCount(10);
        tokyo.setOutboundQueueSize(100);

        LocalWanPublisherStatsImpl singapore = new LocalWanPublisherStatsImpl();
        singapore.setConnected(true);
        singapore.setOutboundQueueSize(200);
        singapore.incrementPublishedEventCount(20);

        LocalWanStatsImpl localWanStats = new LocalWanStatsImpl();
        Map<String, LocalWanPublisherStats> localWanPublisherStatsMap = new HashMap<String, LocalWanPublisherStats>();
        localWanPublisherStatsMap.put("tokyo", tokyo);
        localWanPublisherStatsMap.put("singapore", singapore);
        localWanStats.setLocalPublisherStatsMap(localWanPublisherStatsMap);

        JsonObject serialized = localWanStats.toJson();

        LocalWanStats deserialized = new LocalWanStatsImpl();
        deserialized.fromJson(serialized);

        LocalWanPublisherStats deserializedTokyo = deserialized.getLocalWanPublisherStats().get("tokyo");
        LocalWanPublisherStats deserializedSingapore = deserialized.getLocalWanPublisherStats().get("singapore");

        assertEquals(tokyo.isConnected(), deserializedTokyo.isConnected());
        assertEquals(tokyo.getTotalPublishedEventCount(), deserializedTokyo.getTotalPublishedEventCount());
        assertEquals(tokyo.getOutboundQueueSize(), deserializedTokyo.getOutboundQueueSize());
        assertEquals(tokyo.getTotalPublishLatency(), deserializedTokyo.getTotalPublishLatency());

        assertEquals(singapore.isConnected(), deserializedSingapore.isConnected());
        assertEquals(singapore.getTotalPublishedEventCount(), deserializedSingapore.getTotalPublishedEventCount());
        assertEquals(singapore.getOutboundQueueSize(), deserializedSingapore.getOutboundQueueSize());
        assertEquals(singapore.getTotalPublishLatency(), deserializedSingapore.getTotalPublishLatency());
    }

}
