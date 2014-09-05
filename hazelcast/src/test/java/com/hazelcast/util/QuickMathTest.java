package com.hazelcast.util;


import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigInteger;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link QuickMath} class.
 *
 */
@RunWith(HazelcastParallelClassRunner.class)
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
    public void testMod() {
        int[] aParams = new int[]{
                0, 1, Integer.MAX_VALUE / 2, Integer.MAX_VALUE
        };
        int[] bParams = new int[]{
                1, 2, 1024, powerOfTwo(10), powerOfTwo(20)
        };

        for (int a : aParams) {
            for (int b : bParams) {
                assertEquals(a % b, QuickMath.mod(a, b));
            }
        }
    }

    private int powerOfTwo(int x) {
        return BigInteger.valueOf(2).pow(x).intValue();
    }
}
