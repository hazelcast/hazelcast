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

package com.hazelcast.cardinality.impl.hyperloglog;

import com.hazelcast.cardinality.impl.hyperloglog.impl.HyperLogLogEncoder;
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
public abstract class HyperLogLogEncoderAbstractTest {

    private HyperLogLogEncoder encoder;

    public abstract int runLength();
    public abstract HyperLogLogEncoder createStore();

    @Before
    public void setup() {
        encoder = createStore();
    }

    @Test
    public void add() {
        assertEquals(true, encoder.add(1000L));
        assertEquals(1L, encoder.estimate());
    }

    @Test
    public void addAll() {
        boolean changed = false;
        for (long hash : new long[] { 1L, 1L, 2000L, 3000, 40000L }) {
            changed |= encoder.add(hash);
        }
        assertEquals(true, changed);
        assertEquals(4L, encoder.estimate());
    }

    @Test
    public void addBigRange() {
        int sampleStep = 1000;
        ByteBuffer bb = ByteBuffer.allocate(4);

        for (int i = 1; i <= runLength(); i++) {
            bb.clear();
            bb.putInt(i);
            encoder.add(HashUtil.MurmurHash3_x64_64(bb.array(), 0, bb.array().length));

            if (i % sampleStep == 0) {
                long est = encoder.estimate();
                double errorPct = ((est * 100.0) / i) - 100;
                // 3.0 % is the max error identified during the multi-set tests.
                if (errorPct > 3.0) {
                    fail("Max error reached, for count: " + i + ", got: " + errorPct);
                }
            }
        }
    }
}
