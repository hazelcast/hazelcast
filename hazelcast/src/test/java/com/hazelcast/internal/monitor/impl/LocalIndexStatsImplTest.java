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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalIndexStatsImplTest {

    private LocalIndexStatsImpl stats;

    @Before
    public void setUp() {
        stats = new LocalIndexStatsImpl();

        stats.setCreationTime(1234);
        stats.setHitCount(20);
        stats.setQueryCount(11);
        stats.setAverageHitSelectivity(0.5);
        stats.setAverageHitLatency(81273);
        stats.setInsertCount(91238);
        stats.setTotalInsertLatency(83912);
        stats.setUpdateCount(712639);
        stats.setTotalUpdateLatency(34623);
        stats.setRemoveCount(749274);
        stats.setTotalRemoveLatency(1454957);
        stats.setMemoryCost(2345);
    }

    @Test
    public void testDefaultConstructor() {
        assertEquals(1234, stats.getCreationTime());
        assertEquals(20, stats.getHitCount());
        assertEquals(11, stats.getQueryCount());
        assertEquals(0.5, stats.getAverageHitSelectivity(), 0.01);
        assertEquals(81273, stats.getAverageHitLatency());
        assertEquals(91238, stats.getInsertCount());
        assertEquals(83912, stats.getTotalInsertLatency());
        assertEquals(712639, stats.getUpdateCount());
        assertEquals(34623, stats.getTotalUpdateLatency());
        assertEquals(749274, stats.getRemoveCount());
        assertEquals(1454957, stats.getTotalRemoveLatency());
        assertEquals(2345, stats.getMemoryCost());
        assertNotNull(stats.toString());
    }

}
