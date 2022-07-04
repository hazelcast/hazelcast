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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalExecutorStatsImplTest {

    @Test
    public void testDefaultConstructor() {
        LocalExecutorStatsImpl localExecutorStats = new LocalExecutorStatsImpl();

        assertTrue(localExecutorStats.getCreationTime() > 0);
        assertEquals(0, localExecutorStats.getPendingTaskCount());
        assertEquals(0, localExecutorStats.getStartedTaskCount());
        assertEquals(0, localExecutorStats.getCompletedTaskCount());
        assertEquals(0, localExecutorStats.getCancelledTaskCount());
        assertEquals(0, localExecutorStats.getTotalStartLatency());
        assertEquals(0, localExecutorStats.getTotalExecutionLatency());
        assertNotNull(localExecutorStats.toString());
    }
}
