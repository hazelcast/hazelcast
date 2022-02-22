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
public class LocalQueueStatsImplTest {

    @Test
    public void testDefaultConstructor() {
        LocalQueueStatsImpl localQueueStats = new LocalQueueStatsImpl();
        localQueueStats.setMinAge(13);
        localQueueStats.setMaxAge(28);
        localQueueStats.setAverageAge(18);
        localQueueStats.setOwnedItemCount(1234);
        localQueueStats.setBackupItemCount(15124);

        assertTrue(localQueueStats.getCreationTime() > 0);
        assertEquals(13, localQueueStats.getMinAge());
        assertEquals(28, localQueueStats.getMaxAge());
        assertEquals(18, localQueueStats.getAverageAge());
        assertEquals(1234, localQueueStats.getOwnedItemCount());
        assertEquals(15124, localQueueStats.getBackupItemCount());
        assertNotNull(localQueueStats.toString());
    }
}
