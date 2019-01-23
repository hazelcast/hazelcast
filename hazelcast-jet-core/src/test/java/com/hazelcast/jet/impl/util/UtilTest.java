/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.addClamped;
import static com.hazelcast.jet.impl.util.Util.gcd;
import static com.hazelcast.jet.impl.util.Util.memoizeConcurrent;
import static com.hazelcast.jet.impl.util.Util.subtractClamped;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class UtilTest extends JetTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_addClamped_then_doesNotOverflow() {
        // no overflow
        assertEquals(0, addClamped(0, 0));
        assertEquals(1, addClamped(1, 0));
        assertEquals(-1, addClamped(-1, 0));
        assertEquals(-1, addClamped(Long.MAX_VALUE, Long.MIN_VALUE));
        assertEquals(-1, addClamped(Long.MIN_VALUE, Long.MAX_VALUE));

        // overflow over MAX_VALUE
        assertEquals(Long.MAX_VALUE, addClamped(Long.MAX_VALUE, 1));
        assertEquals(Long.MAX_VALUE, addClamped(Long.MAX_VALUE, Long.MAX_VALUE));

        // overflow over MIN_VALUE
        assertEquals(Long.MIN_VALUE, addClamped(Long.MIN_VALUE, -1));
        assertEquals(Long.MIN_VALUE, addClamped(Long.MIN_VALUE, Long.MIN_VALUE));
    }

    @Test
    public void when_subtractClamped_then_doesNotOverflow() {
        // no overflow
        assertEquals(0, subtractClamped(0, 0));
        assertEquals(1, subtractClamped(1, 0));
        assertEquals(-1, subtractClamped(-1, 0));
        assertEquals(0, subtractClamped(Long.MAX_VALUE, Long.MAX_VALUE));
        assertEquals(0, subtractClamped(Long.MIN_VALUE, Long.MIN_VALUE));

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
    public void test_calculateGcd2() {
        assertEquals(2, gcd(0L, 2L));
        assertEquals(1, gcd(1L, 2L));
        assertEquals(2, gcd(2L, 4L));
        assertEquals(2, gcd(-2L, 4L));
    }

    @Test
    public void test_calculateGcdN() {
        assertEquals(0, gcd());
        assertEquals(4, gcd(4, 4, 4));
        assertEquals(4, gcd(4, 8, 12));
        assertEquals(1, gcd(4, 8, 13));
    }

    @Test
    public void test_copyMap() throws Exception {
        JetInstance[] instances = createJetMembers(2);

        logger.info("Populating source map...");
        IMapJet<Object, Object> srcMap = instances[0].getMap("src");
        Map<Integer, Integer> testData = IntStream.range(0, 100_000).boxed().collect(toMap(e -> e, e -> e));
        srcMap.putAll(testData);

        logger.info("Copying using job...");
        Util.copyMapUsingJob(instances[0], 128, srcMap.getName(), "target").get();
        logger.info("Done copying");

        assertEquals(testData, new HashMap<>(instances[0].getMap("target")));
    }
}
