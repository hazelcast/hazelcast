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

package com.hazelcast.jet.impl.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.addClamped;
import static com.hazelcast.jet.impl.util.Util.memoizeConcurrent;
import static com.hazelcast.jet.impl.util.Util.subtractClamped;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class UtilTest {

    @Test
    public void when_addClamped_then_doesntOverflow() {
        // no overflow
        assertEquals(0, addClamped(0, 0));
        assertEquals(1, addClamped(1, 0));
        assertEquals(-1, addClamped(-1, 0));
        assertEquals(-1, addClamped(Long.MAX_VALUE, Long.MIN_VALUE));

        // overflow over MAX_VALUE
        assertEquals(Long.MAX_VALUE, addClamped(Long.MAX_VALUE, 1));
        assertEquals(Long.MAX_VALUE, addClamped(Long.MAX_VALUE, Long.MAX_VALUE));

        // overflow over MIN_VALUE
        assertEquals(Long.MIN_VALUE, addClamped(Long.MIN_VALUE, -1));
        assertEquals(Long.MIN_VALUE, addClamped(Long.MIN_VALUE, Long.MIN_VALUE));
    }

    @Test
    public void when_subtractClamped_then_doesntOverflow() {
        // no overflow
        assertEquals(0, subtractClamped(0, 0));
        assertEquals(1, subtractClamped(1, 0));
        assertEquals(-1, subtractClamped(-1, 0));
        assertEquals(0, subtractClamped(Long.MAX_VALUE, Long.MAX_VALUE));

        // overflow over MAX_VALUE
        assertEquals(Long.MAX_VALUE, subtractClamped(Long.MAX_VALUE, -1));
        assertEquals(Long.MAX_VALUE, subtractClamped(Long.MAX_VALUE, Long.MIN_VALUE));

        // overflow over MIN_VALUE
        assertEquals(Long.MIN_VALUE, subtractClamped(Long.MIN_VALUE, 1));
        assertEquals(Long.MIN_VALUE, subtractClamped(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Test
    public void when_memoizeConcurrent_then_threadSafe() {
        final Object obj = new Object();
        Supplier<Object> supplier = new Supplier<Object>() {
            boolean supplied;
            @Override
            public Object get() {
               if (supplied) {
                   throw new IllegalStateException("Supplier was already called once.");
               }
               supplied = true;
               return obj;
            }
        };

        // does not fail 100% with non-concurrent memoize, but about 50% of the time.
        List<Object> list = Stream.generate(memoizeConcurrent(supplier)).limit(4).parallel().collect(Collectors.toList());
        assertTrue("Not all objects matched expected", list.stream().allMatch(o -> o.equals(obj)));
    }

    @Test(expected = NullPointerException.class)
    public void when_memoizeConcurrentWithNullSupplier_then_exception() {
       Supplier<Object> supplier = () -> null;
       memoizeConcurrent(supplier).get();
    }

    @Test
    public void test_idToString() {
        assertEquals("0000-0000-0000-0000", Util.idToString(0));
        assertEquals("0000-0000-0000-0001", Util.idToString(1));
        assertEquals("7fff-ffff-ffff-ffff", Util.idToString(Long.MAX_VALUE));
        assertEquals("8000-0000-0000-0000", Util.idToString(Long.MIN_VALUE));
        assertEquals("ffff-ffff-ffff-ffff", Util.idToString(-1));
        assertEquals("1122-10f4-7de9-8115", Util.idToString(1234567890123456789L));
        assertEquals("eedd-ef0b-8216-7eeb", Util.idToString(-1234567890123456789L));
    }
}
