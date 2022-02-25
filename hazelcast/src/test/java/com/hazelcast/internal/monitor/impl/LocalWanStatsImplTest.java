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

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.monitor.LocalWanPublisherStats;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.json.internal.JsonSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanPublisherState;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalWanStatsImplTest {

    @Test
    public void testSerialization() {
        LocalWanPublisherStatsImpl tokyo = new LocalWanPublisherStatsImpl();
        tokyo.setConnected(true);
        tokyo.incrementPublishedEventCount(10);
        tokyo.setOutboundQueueSize(100);
        tokyo.setState(WanPublisherState.REPLICATING);

        LocalWanPublisherStatsImpl singapore = new LocalWanPublisherStatsImpl();
        singapore.setConnected(true);
        singapore.setOutboundQueueSize(200);
        singapore.incrementPublishedEventCount(20);
        singapore.setState(WanPublisherState.REPLICATING);

        LocalWanStatsImpl localWanStats = new LocalWanStatsImpl();
        Map<String, LocalWanPublisherStats> localWanPublisherStatsMap = new HashMap<String, LocalWanPublisherStats>();
        localWanPublisherStatsMap.put("tokyo", tokyo);
        localWanPublisherStatsMap.put("singapore", singapore);
        localWanStats.setLocalPublisherStatsMap(localWanPublisherStatsMap);

        JsonObject serialized = localWanStats.toJson();

        LocalWanStats deserialized = new LocalWanStatsImpl();
        ((JsonSerializable) deserialized).fromJson(serialized);

        LocalWanPublisherStats deserializedTokyo = deserialized.getLocalWanPublisherStats().get("tokyo");
        LocalWanPublisherStats deserializedSingapore = deserialized.getLocalWanPublisherStats().get("singapore");

        assertEquals(tokyo.isConnected(), deserializedTokyo.isConnected());
        assertEquals(tokyo.getPublisherState(), deserializedTokyo.getPublisherState());

        assertEquals(singapore.isConnected(), deserializedSingapore.isConnected());
        assertEquals(singapore.getPublisherState(), deserializedSingapore.getPublisherState());
    }

}
