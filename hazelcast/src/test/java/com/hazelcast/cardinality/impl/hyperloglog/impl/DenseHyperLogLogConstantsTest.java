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
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Checks the consistency of {@link DenseHyperLogLogConstants} with hashcodes.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DenseHyperLogLogConstantsTest extends HazelcastTestSupport {

    private static final int THRESHOLD_HASHCODE = -1946099911;
    private static final int[] RAW_ESTIMATE_DATA_HASHCODES = {
            -1251035322,
            -1094734953,
            -61611651,
            -40264626,
            479598381,
            -116264945,
            1050131386,
            -1235040548,
            -1202239017,
            1288152491,
            -769393172,
            -1652552964,
            -497616505,
            -1689057893,
            923172265,
    };
    private static final int[] BIAS_DATA_HASHCODES = {
            -1077449490,
            1779769334,
            -1948875718,
            1532988461,
            -990299124,
            -591836144,
            48144655,
            -470742222,
            -1450150050,
            1929284635,
            -697321875,
            -1556078395,
            -1405633222,
            -88240126,
            1330624843,
    };

    @Test
    public void testConstructor() {
        assertUtilityConstructor(DenseHyperLogLogConstants.class);
    }

    @Test
    public void testHashCodes() {
        assertEquals("DenseHyperLogLogConstants.THRESHOLD",
                THRESHOLD_HASHCODE, Arrays.hashCode(DenseHyperLogLogConstants.THRESHOLD));
        assertArrayHashcodes("RAW_ESTIMATE_DATA", DenseHyperLogLogConstants.RAW_ESTIMATE_DATA, RAW_ESTIMATE_DATA_HASHCODES);
        assertArrayHashcodes("BIAS_DATA", DenseHyperLogLogConstants.BIAS_DATA, BIAS_DATA_HASHCODES);
    }

    /**
     * Asserts {@code double[][]} arrays by an array of hash codes per sub-array.
     * <p>
     * The method {@link Arrays#hashCode(Object[])} is not constant for {@code double[][]} arrays.
     * So we compare each sub-array on its own with {@link Arrays#hashCode(double[])}.
     */
    private static void assertArrayHashcodes(String label, double[][] array, int[] hashcodes) {
        assertEquals(label + " and hashcode array lengths differ", hashcodes.length, array.length);
        for (int i = 0; i < array.length; i++) {
            assertEquals("DenseHyperLogLogConstants." + label + " at index " + i,
                    hashcodes[i], Arrays.hashCode(array[i]));
        }
    }
}
