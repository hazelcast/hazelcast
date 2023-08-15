/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.bitmap;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import java.util.stream.Stream;

/**
 * Assert that the new {@code toUnsigned} implementations return the same results as the previous implementations
 *
 * @see <a href="https://github.com/hazelcast/hazelcast/issues/25216">GitHub issue</a>
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BitmapUtilsTest {

    private static int shortToIntMask;
    private static long shortToLongMask;
    private static long intToLongMask;

    @BeforeClass
    public static void setUp() {
        shortToIntMask = 0xFFFF;
        shortToLongMask = 0xFFFFL;
        intToLongMask = 0xFFFFFFFFL;
    }

    @Test
    public void testShortToUnsignedInt() {
        getShortTestData().forEach(
                val -> assertEquals(val.toString(), toUnsignedIntPreviousImplementation(val), BitmapUtils.toUnsignedInt(val)));
    }

    /**
     * Does exactly the same thing as Java 8 Short.toUnsignedInt.
     */
    public static int toUnsignedIntPreviousImplementation(short value) {
        return ((int) value) & shortToIntMask;
    }

    @Test
    public void testShortToUnsignedLong() {
        getShortTestData().forEach(val -> assertEquals(val.toString(), toUnsignedLongPreviousImplementation(val),
                BitmapUtils.toUnsignedLong(val)));
    }

    /**
     * Does exactly the same thing as Java 8 Short.toUnsignedLong.
     */
    public static long toUnsignedLongPreviousImplementation(short value) {
        return ((long) value) & shortToLongMask;
    }

    @Test
    public void testIntToUnsignedLong() {
        getIntegerTestData().forEach(val -> assertEquals(val.toString(), toUnsignedLongPreviousImplementation(val),
                BitmapUtils.toUnsignedLong(val)));
    }

    /**
     * Does exactly the same thing as Java 8 Integer.toUnsignedLong.
     */
    public static long toUnsignedLongPreviousImplementation(int value) {
        return ((long) value) & intToLongMask;
    }

    private static Stream<Short> getShortTestData() {
        return Stream.of(Short.MIN_VALUE, (short) -1, (short) 0, (short) 1, Short.MAX_VALUE);
    }

    private static Stream<Integer> getIntegerTestData() {
        return Stream.of(Integer.MIN_VALUE, (int) Short.MIN_VALUE, -1, 0, 1, (int) Short.MAX_VALUE, Integer.MAX_VALUE);
    }
}
