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

package com.hazelcast.cardinality.impl.hyperloglog.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DenseHyperLogLogEncoderTest extends HyperLogLogEncoderAbstractTest {

    @Override
    public int precision() {
        return 14;
    }

    @Override
    public HyperLogLogEncoder createStore() {
        return new DenseHyperLogLogEncoder(precision());
    }

    @Override
    public int runLength() {
        return 10000000;
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void testAdd_assertRegisterLength() {
        DenseHyperLogLogEncoder encoder = new DenseHyperLogLogEncoder(precision(), new byte[0]);

        encoder.add(5);
    }

    @Test
    public void testGetMemoryFootprint() {
        DenseHyperLogLogEncoder encoder = getDenseHyperLogLogEncoder();
        int memoryFootprint = encoder.getMemoryFootprint();

        assertEquals(1 << precision(), memoryFootprint);
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void testAlpha_withGivenZeroAsInvalidMemoryFootprint() {
        DenseHyperLogLogEncoder encoder = new DenseHyperLogLogEncoder(0);
        encoder.estimate();
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void testAlpha_withInvalidMemoryFootprint() {
        DenseHyperLogLogEncoder encoder = new DenseHyperLogLogEncoder(1);

        encoder.estimate();
    }

    @Test
    public void testAlpha_withMemoryFootprintOf16() {
        DenseHyperLogLogEncoder encoder = new DenseHyperLogLogEncoder(4);

        encoder.estimate();
    }

    @Test
    public void testAlpha_withMemoryFootprintOf32() {
        DenseHyperLogLogEncoder encoder = new DenseHyperLogLogEncoder(5);

        encoder.estimate();
    }

    @Test
    public void testAlpha_withMemoryFootprintOf64() {
        DenseHyperLogLogEncoder encoder = new DenseHyperLogLogEncoder(6);

        encoder.estimate();
    }

    @Test
    public void testAlpha_withMemoryFootprintOf128() {
        DenseHyperLogLogEncoder encoder = new DenseHyperLogLogEncoder(7);

        encoder.estimate();
    }

    private DenseHyperLogLogEncoder getDenseHyperLogLogEncoder() {
        return (DenseHyperLogLogEncoder) getEncoder();
    }
}
