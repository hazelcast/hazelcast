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

package com.hazelcast.internal.memory;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.memory.MemoryStatsSupport.freePhysicalMemory;
import static com.hazelcast.internal.memory.MemoryStatsSupport.freeSwapSpace;
import static com.hazelcast.internal.memory.MemoryStatsSupport.totalPhysicalMemory;
import static com.hazelcast.internal.memory.MemoryStatsSupport.totalSwapSpace;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemoryStatsSupportTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(MemoryStatsSupport.class);
    }

    @Test
    public void testTotalPhysicalMemory() {
        assertTrue(totalPhysicalMemory() >= -1);
    }

    @Test
    public void testFreePhysicalMemory() {
        assertTrue(freePhysicalMemory() >= -1);
    }

    @Test
    public void testTotalSwapSpace() {
        assertTrue(totalSwapSpace() >= -1);
    }

    @Test
    public void testFreeSwapSpace() {
        assertTrue(freeSwapSpace() >= -1);
    }
}
