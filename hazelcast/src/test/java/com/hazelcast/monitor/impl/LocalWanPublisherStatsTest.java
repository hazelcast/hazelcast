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
