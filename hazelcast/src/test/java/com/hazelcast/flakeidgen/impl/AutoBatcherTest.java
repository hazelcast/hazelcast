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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static com.hazelcast.flakeidgen.impl.FlakeIdConcurrencyTestUtil.IDS_IN_THREAD;
import static com.hazelcast.flakeidgen.impl.FlakeIdConcurrencyTestUtil.NUM_THREADS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AutoBatcherTest {

    private static final int VALIDITY = 10000;

    private AutoBatcher batcher = new AutoBatcher(3, VALIDITY, new AutoBatcher.IdBatchSupplier() {

        int base;

        @Override
        public IdBatch newIdBatch(int batchSize) {
            try {
                return new IdBatch(base, 1, batchSize);
            } finally {
                base += batchSize;
            }
        }
    });

    @Test
    public void when_validButUsedAll_then_fetchNew() {
        assertEquals(0, batcher.newId());
        assertEquals(1, batcher.newId());
        assertEquals(2, batcher.newId());
        assertEquals(3, batcher.newId());
    }

    @Test
    public void when_notValid_then_fetchNew() throws Exception {
        assertEquals(0, batcher.newId());
        Thread.sleep(VALIDITY);
        assertEquals(3, batcher.newId());
    }

    @Test
    public void concurrencySmokeTest() throws Exception {
        Set<Long> ids = FlakeIdConcurrencyTestUtil.concurrentlyGenerateIds(() -> batcher.newId());
        for (int i = 0; i < NUM_THREADS * IDS_IN_THREAD; i++) {
            assertTrue("Missing ID: " + i, ids.contains((long) i));
        }
    }
}
