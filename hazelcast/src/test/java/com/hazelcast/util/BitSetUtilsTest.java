/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BitSetUtilsTest extends HazelcastTestSupport {

    private static final int SIZE = 10;

    private BitSet bitSet;
    private List<Integer> indexes;

    @Before
    public void setUp() {
        bitSet = new BitSet(SIZE);

        indexes = new ArrayList<Integer>(SIZE);
        for (int i = 0; i < SIZE; i++) {
            indexes.add(i);
        }
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(BitSetUtils.class);
    }

    @Test
    public void hasAtLeastOneBitSet_whenEmptyBitSet_thenReturnFalse() {
        boolean isBitSet = BitSetUtils.hasAtLeastOneBitSet(bitSet, indexes);
        assertFalse(isBitSet);
    }

    @Test
    public void hasAtLeastOneBitSet_whenBitIsSet_thenReturnTrue() {
        for (int position = 0; position < SIZE; position++) {
            bitSet = new BitSet(SIZE);
            bitSet.set(position);
            boolean isBitSet = BitSetUtils.hasAtLeastOneBitSet(bitSet, indexes);
            assertTrue(isBitSet);
        }
    }

    @Test
    public void setBits_thenSetAllBits() {
        BitSetUtils.setBits(bitSet, indexes);
        assertBitsAtPositionsAreSet(bitSet, indexes);
    }


    @Test
    public void hasAllBitsSet_true() {
        bitSet.set(1);
        bitSet.set(3);
        bitSet.set(5);

        assertTrue(BitSetUtils.hasAllBitsSet(bitSet, asList(1, 3, 5)));
        assertTrue(BitSetUtils.hasAllBitsSet(bitSet, asList(3, 5)));
    }

    @Test
    public void hasAllBitsSet_false() {
        bitSet.set(1);
        bitSet.set(3);
        bitSet.set(5);

        assertFalse(BitSetUtils.hasAllBitsSet(bitSet, asList(2, 4, 6)));
        assertFalse(BitSetUtils.hasAllBitsSet(bitSet, asList(3, 5, 6)));
    }

    @Test
    public void unsetBits() {
        bitSet.set(0, 4);
        BitSetUtils.unsetBits(bitSet, asList(0, 1, 2, 3, 4));

        assertTrue(bitSet.isEmpty());
    }

    @Test
    public void unsetBits_individual() {
        bitSet.set(0, 2);
        BitSetUtils.unsetBits(bitSet, asList(0));

        assertFalse(bitSet.get(0));
        assertTrue(bitSet.get(1));
    }

    // public static void unsetBits(BitSet bitSet, Iterable<Integer> indexes);


    private static void assertBitsAtPositionsAreSet(BitSet bitSet, List<Integer> indexes) {
        for (int index : indexes) {
            boolean isSet = bitSet.get(index);
            assertTrue(isSet);
        }
    }
}
