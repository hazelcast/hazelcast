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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link QuickMath} class.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class QuickMathTest {

    @Test
    public void testIsPowerOfTwo() {
        assertTrue(QuickMath.isPowerOfTwo(1));
        assertTrue(QuickMath.isPowerOfTwo(2));
        assertFalse(QuickMath.isPowerOfTwo(3));
        assertTrue(QuickMath.isPowerOfTwo(1024));
        assertFalse(QuickMath.isPowerOfTwo(1023));
        assertFalse(QuickMath.isPowerOfTwo(Integer.MAX_VALUE));
    }

    @Test
    public void testNextPowerOfTwo() {
        assertEquals(1, QuickMath.nextPowerOfTwo(-9999999));
        assertEquals(1, QuickMath.nextPowerOfTwo(-1));
        assertEquals(1, QuickMath.nextPowerOfTwo(0));
        assertEquals(1, QuickMath.nextPowerOfTwo(1));
        assertEquals(2, QuickMath.nextPowerOfTwo(2));
        assertEquals(1024, QuickMath.nextPowerOfTwo(999));
        assertEquals(1 << 23, QuickMath.nextPowerOfTwo((1 << 23) - 1));
        assertEquals(1 << 23, QuickMath.nextPowerOfTwo(1 << 23));

        assertEquals(2048L, QuickMath.nextPowerOfTwo(2000L));
        assertEquals(1L << 33, QuickMath.nextPowerOfTwo((1L << 33) - 3));
        assertEquals(1L << 43, QuickMath.nextPowerOfTwo((1L << 43)));
    }

    @Test
    public void testNextLongPowerOfTwo() {
        assertEquals(1L, QuickMath.nextPowerOfTwo(-9999999L));
        assertEquals(1L, QuickMath.nextPowerOfTwo(-1L));
        assertEquals(1L, QuickMath.nextPowerOfTwo(0L));
        assertEquals(1L, QuickMath.nextPowerOfTwo(1L));
        assertEquals(2L, QuickMath.nextPowerOfTwo(2L));
        assertEquals(4L, QuickMath.nextPowerOfTwo(3L));
        assertEquals(1L << 62, QuickMath.nextPowerOfTwo((1L << 61) + 1));
    }

    @Test
    public void testModPowerOfTwo() {
        int[] aParams = new int[]{
                0,
                1,
                Integer.MAX_VALUE / 2,
                Integer.MAX_VALUE,
        };
        int[] bParams = new int[]{
                1,
                2,
                1024,
                powerOfTwo(10),
                powerOfTwo(20),
        };

        for (int a : aParams) {
            for (int b : bParams) {
                assertEquals(a % b, QuickMath.modPowerOfTwo(a, b));
            }
        }
    }

    private int powerOfTwo(int x) {
        return BigInteger.valueOf(2).pow(x).intValue();
    }
}
