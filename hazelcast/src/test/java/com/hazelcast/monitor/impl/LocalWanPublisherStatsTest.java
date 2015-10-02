package com.hazelcast.monitor.impl;


import com.eclipsesource.json.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LocalWanPublisherStatsTest {

    @Test
    public void testSerialization() {
        LocalWanPublisherStatsImpl localWanPublisherStats = new LocalWanPublisherStatsImpl();
        localWanPublisherStats.setConnected(true);
        localWanPublisherStats.setOutboundRecsSec(15);
        localWanPublisherStats.setOutboundQueueSize(100);
        localWanPublisherStats.setOutboundLatencyMs(13);

        JsonObject serialized = localWanPublisherStats.toJson();

        LocalWanPublisherStatsImpl deserialized = new LocalWanPublisherStatsImpl();
        deserialized.fromJson(serialized);

        assertEquals(localWanPublisherStats.isConnected(), deserialized.isConnected());
        assertEquals(localWanPublisherStats.getOutboundLatencyMs(), deserialized.getOutboundLatencyMs());
        assertEquals(localWanPublisherStats.getOutboundQueueSize(), deserialized.getOutboundQueueSize());
        assertEquals(localWanPublisherStats.getOutboundRecsSec(), deserialized.getOutboundRecsSec());
    }

}
