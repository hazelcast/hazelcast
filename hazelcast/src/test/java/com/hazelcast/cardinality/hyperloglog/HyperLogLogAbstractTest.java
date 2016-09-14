/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cardinality.hyperloglog;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.HashUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public abstract class HyperLogLogAbstractTest {

    private HyperLogLog store;

    public abstract HyperLogLog createStore();

    @Before
    public void setup() {
        store = createStore();
    }

    @Test
    public void aggregate() {
        assertEquals(true, store.aggregate(1000L));
        assertEquals(1L, store.estimate());
    }

    @Test
    public void aggregateAll() {
        assertEquals(true, store.aggregateAll(new long[] { 1L, 2000L, 3000, 40000L }));
        assertEquals(4L, store.estimate());
    }

    @Test
    public void aggregateBigRange() {
        int sampleStep = 1000;
        ByteBuffer bb = ByteBuffer.allocate(4);

        for (int i=1; i<=10000000; i++) {
            bb.clear();
            bb.putInt(i);
            store.aggregate(HashUtil.MurmurHash3_x64_64(bb.array(), 0, bb.array().length));

            if (i % sampleStep == 0) {
                long est = store.estimate();
                double errorPct = ((est * 100.0) / i) - 100;
                // 3.0 % is the max error identified during the multi-set tests.
                if (errorPct > 3.0) {
                    fail("Max error reached, for count: " + i + ", got: " + errorPct);
                }
            }
        }
    }
}
