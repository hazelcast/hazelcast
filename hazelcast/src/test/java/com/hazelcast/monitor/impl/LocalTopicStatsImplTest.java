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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalTopicStatsImplTest {

    private LocalTopicStatsImpl localTopicStats;

    @Before
    public void setUp() {
        localTopicStats = new LocalTopicStatsImpl();

        localTopicStats.incrementPublishes();
        localTopicStats.incrementPublishes();
        localTopicStats.incrementPublishes();
        localTopicStats.incrementReceives();
        localTopicStats.incrementReceives();
    }

    @Test
    public void testDefaultConstructor() {
        assertTrue(localTopicStats.getCreationTime() > 0);
        assertEquals(3, localTopicStats.getPublishOperationCount());
        assertEquals(2, localTopicStats.getReceiveOperationCount());
        assertNotNull(localTopicStats.toString());
    }

    @Test
    public void testSerialization() {
        JsonObject serialized = localTopicStats.toJson();
        LocalTopicStatsImpl deserialized = new LocalTopicStatsImpl();
        deserialized.fromJson(serialized);

        assertTrue(deserialized.getCreationTime() > 0);
        assertEquals(3, deserialized.getPublishOperationCount());
        assertEquals(2, deserialized.getReceiveOperationCount());
        assertNotNull(deserialized.toString());
    }
}
