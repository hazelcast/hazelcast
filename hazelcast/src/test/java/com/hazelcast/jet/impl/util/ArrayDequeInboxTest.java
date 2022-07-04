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

package com.hazelcast.jet.impl.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ArrayDequeInboxTest {

    private static final List<Integer> ITEMS = asList(1, 2);
    private static final Function<Object, Integer> DOUBLE_INT = n -> (int) n * 2;

    private ArrayDequeInbox inbox = new ArrayDequeInbox(new ProgressTracker());

    @Before
    public void before() {
        inbox.queue().addAll(ITEMS);
    }

    @Test
    public void when_pollNonEmpty_then_getItem() {
        assertEquals(ITEMS.get(0), inbox.poll());
    }

    @Test
    public void when_pollEmpty_then_getNull() {
        inbox.clear();
        assertNull(inbox.poll());
    }

    @Test
    public void when_removeNonEmpty_then_removeItem() {
        inbox.remove();
        assertEquals(ITEMS.get(1), inbox.poll());
    }

    @Test(expected = NoSuchElementException.class)
    public void when_removeEmpty_then_getException() {
        inbox.clear();
        inbox.remove();
    }

    @Test
    public void when_drainToCollection_then_allDrained() {
        ArrayList<Object> sink = new ArrayList<>();
        inbox.drainTo(sink);
        assertEquals(ITEMS, sink);
    }

    @Test
    public void test_drainToCollectionWithLimit_0() {
        test_drainToCollectionWithLimit(0);
    }

    @Test
    public void test_drainToCollectionWithLimit_1() {
        test_drainToCollectionWithLimit(1);
    }

    @Test
    public void test_drainToCollectionWithLimit_2() {
        test_drainToCollectionWithLimit(2);
    }

    @Test
    public void test_drainToCollectionWithLimit_3() {
        test_drainToCollectionWithLimit(3);
    }

    private void test_drainToCollectionWithLimit(int n) {
        ArrayList<Object> sink = new ArrayList<>();
        inbox.drainTo(sink, n);
        assertEquals(ITEMS.subList(0, Math.min(n, ITEMS.size())), sink);
    }

    @Test
    public void test_drainToCollectionWithLimitAndMapper_0() {
        test_drainToCollectionWithLimitAndMapper(0);
    }

    @Test
    public void test_drainToCollectionWithLimitAndMapper_1() {
        test_drainToCollectionWithLimitAndMapper(1);
    }

    @Test
    public void test_drainToCollectionWithLimitAndMapper_2() {
        test_drainToCollectionWithLimitAndMapper(2);
    }

    @Test
    public void test_drainToCollectionWithLimitAndMapper_3() {
        test_drainToCollectionWithLimitAndMapper(3);
    }

    private void test_drainToCollectionWithLimitAndMapper(int n) {
        List<Integer> sink = new ArrayList<>();
        inbox.drainTo(sink, n, DOUBLE_INT);
        assertEquals(ITEMS.stream().limit(n).map(DOUBLE_INT).collect(toList()), sink);
    }

    @Test
    public void when_drainToConsumer_then_allDrained() {
        ArrayList<Object> sink = new ArrayList<>();
        inbox.drain(sink::add);
        assertEquals(ITEMS, sink);
    }

    @Test
    public void when_iterator_then_allIterated() {
        ArrayList<Object> actual = new ArrayList<>();
        for (Object o : inbox) {
            actual.add(o);
        }
        assertEquals(ITEMS, actual);
    }
}
