package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LocalWanStatsImplTest {

    @Test
    public void testSerialization() {
        LocalWanPublisherStatsImpl tokyo = new LocalWanPublisherStatsImpl();
        tokyo.setConnected(true);
        tokyo.setOutboundRecsSec(15);
        tokyo.setOutboundQueueSize(100);
        tokyo.setOutboundLatencyMs(13);

        LocalWanPublisherStatsImpl singapore = new LocalWanPublisherStatsImpl();
        singapore.setConnected(true);
        singapore.setOutboundRecsSec(25);
        singapore.setOutboundQueueSize(200);
        singapore.setOutboundLatencyMs(15);

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
        assertEquals(tokyo.getOutboundLatencyMs(), deserializedTokyo.getOutboundLatencyMs());
        assertEquals(tokyo.getOutboundQueueSize(), deserializedTokyo.getOutboundQueueSize());
        assertEquals(tokyo.getOutboundRecsSec(), deserializedTokyo.getOutboundRecsSec());

        assertEquals(singapore.isConnected(), deserializedSingapore.isConnected());
        assertEquals(singapore.getOutboundLatencyMs(), deserializedSingapore.getOutboundLatencyMs());
        assertEquals(singapore.getOutboundQueueSize(), deserializedSingapore.getOutboundQueueSize());
        assertEquals(singapore.getOutboundRecsSec(), deserializedSingapore.getOutboundRecsSec());
    }

}
