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

package com.hazelcast.internal.metrics.sources;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.metrics.AbstractMetricsTest;
import com.hazelcast.memory.DefaultMemoryStats;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MemoryMetricsTest extends AbstractMetricsTest {

    @Before
    public void setup() {
        registry.register(new MemoryMetrics(new DefaultMemoryStats()));
    }

    @Test
    public void totalPhysical() {
        assertProbed("memory.totalPhysical", -1L);
    }

    @Test
    public void freePhysical() {
        assertProbed("memory.freePhysical", -1L);
    }

    @Test
    public void maxHeap() {
        assertProbed("memory.maxHeap");
    }

    @Test
    public void committedHeap() {
        assertProbed("memory.committedHeap");
    }

    @Test
    public void usedHeap() {
        assertProbed("memory.usedHeap");
    }

    @Test
    public void freeHeap() {
        assertProbed("memory.freeHeap");
    }

    @Test
    public void maxNative() {
        assertProbed("memory.maxNative");
    }

    @Test
    public void committedNative() {
        assertProbed("memory.committedNative");
    }

    @Test
    public void usedNative() {
        assertProbed("memory.usedNative");
    }

    @Test
    public void freeNative() {
        assertProbed("memory.freeNative");
    }
}
