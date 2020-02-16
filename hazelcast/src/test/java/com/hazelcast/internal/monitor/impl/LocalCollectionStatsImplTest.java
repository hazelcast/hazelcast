/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.Clock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertBetween;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalCollectionStatsImplTest {

    @Test
    public void testDefaultConstructor() {
        LocalCollectionStatsImpl localListStats = createTestStats();

        assertTrue(localListStats.getCreationTime() > 0);
        assertEquals(123456, localListStats.getLastUpdateTime());
        assertEquals(654321, localListStats.getLastAccessTime());
        assertEquals(1, localListStats.getNumberOfHits());
    }

    @Test
    public void testCreationTime() {
        long beforeCreationTime = Clock.currentTimeMillis();
        LocalCollectionStatsImpl localListStats = createTestStats();
        long afterCreationTime = Clock.currentTimeMillis();

        assertBetween("creationTime", localListStats.getCreationTime(), beforeCreationTime, afterCreationTime);
    }

    @Test
    public void testSerialization() {
        LocalCollectionStatsImpl localListStats = createTestStats();

        JsonObject serialized = localListStats.toJson();
        LocalCollectionStatsImpl deserialized = new LocalCollectionStatsImpl();
        deserialized.fromJson(serialized);

        assertTrue(localListStats.getCreationTime() > 0);
        assertEquals(123456, localListStats.getLastUpdateTime());
        assertEquals(654321, localListStats.getLastAccessTime());
        assertEquals(1, localListStats.getNumberOfHits());
    }

    private LocalCollectionStatsImpl createTestStats() {
        LocalCollectionStatsImpl localListStats = new LocalCollectionStatsImpl();
        localListStats.setLastUpdateTime(123456);
        localListStats.setLastAccessTime(654321);
        localListStats.incrementNumberOfHits();
        return localListStats;
    }
}
