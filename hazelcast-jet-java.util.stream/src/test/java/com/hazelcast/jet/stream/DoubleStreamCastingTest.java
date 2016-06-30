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

package com.hazelcast.jet.stream;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.stream.DoubleStream;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class DoubleStreamCastingTest extends JetStreamTestSupport {

    private DoubleStream stream;

    @Before
    public void setUp() {
        IStreamList<Integer> list = getStreamList(instance);
        stream = list.stream().mapToDouble(m -> m);
    }

    @Test(expected = ClassCastException.class)
    public void testMap() {
        stream.map(m -> m);
    }

    @Test(expected = ClassCastException.class)
    public void testFlatMap() {
        stream.flatMap(DoubleStream::of);
    }

    @Test(expected = ClassCastException.class)
    public void testCollect() {
        stream.collect(() -> new Double[]{0D},
                (r, e) -> r[0] += e,
                (a, b) -> a[0] += b[0]);
    }

    @Test(expected = ClassCastException.class)
    public void testForEach() {
        stream.forEach(System.out::println);
    }

    @Test(expected = ClassCastException.class)
    public void testForEachOrdered() {
        stream.forEachOrdered(System.out::println);
    }

    @Test(expected = ClassCastException.class)
    public void testAllMatch() {
        stream.allMatch(m -> true);
    }

    @Test(expected = ClassCastException.class)
    public void testAnyMatch() {
        stream.anyMatch(m -> true);
    }

    @Test(expected = ClassCastException.class)
    public void testNoneMatch() {
        stream.noneMatch(m -> true);
    }

    @Test(expected = ClassCastException.class)
    public void testFilter() {
        stream.filter(m -> true);
    }

    @Test(expected = ClassCastException.class)
    public void testMapToObj() {
        stream.mapToObj(m -> m);
    }

    @Test(expected = ClassCastException.class)
    public void testMapToInt() {
        stream.mapToInt(m -> (int) m);
    }

    @Test(expected = ClassCastException.class)
    public void testMapToLong() {
        stream.mapToLong(m -> (long) m);
    }

    @Test(expected = ClassCastException.class)
    public void testPeek() {
        stream.peek(System.out::println);
    }

    @Test(expected = ClassCastException.class)
    public void testReduce() {
        stream.reduce((l, r) -> l + r);
    }

    @Test(expected = ClassCastException.class)
    public void testReduce2() {
        stream.reduce(0, (l, r) -> l + r);
    }
}