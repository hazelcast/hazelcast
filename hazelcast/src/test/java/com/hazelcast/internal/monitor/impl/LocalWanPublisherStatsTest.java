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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanPublisherState;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalWanPublisherStatsTest {

    @Test
    public void testSerialization() {
        LocalWanPublisherStatsImpl localWanPublisherStats = new LocalWanPublisherStatsImpl();
        localWanPublisherStats.setConnected(true);
        localWanPublisherStats.setOutboundQueueSize(100);
        localWanPublisherStats.incrementPublishedEventCount(10);
        localWanPublisherStats.setState(WanPublisherState.REPLICATING);

        JsonObject serialized = localWanPublisherStats.toJson();

        LocalWanPublisherStatsImpl deserialized = new LocalWanPublisherStatsImpl();
        deserialized.fromJson(serialized);

        assertEquals(localWanPublisherStats.isConnected(), deserialized.isConnected());
        assertEquals(localWanPublisherStats.getPublisherState(), deserialized.getPublisherState());
    }

}
