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

package com.hazelcast.jet;

import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
public class ProcessorsTest {
    private ArrayDequeInbox inbox;
    private ArrayDequeOutbox outbox;
    private Queue<Object> bucket;
    private Context context;

    @Before
    public void before() {
        inbox = new ArrayDequeInbox();
        outbox = new ArrayDequeOutbox(1, new int[]{1}, new ProgressTracker());
        context = mock(Context.class);
        bucket = outbox.queueWithOrdinal(0);
    }

    @Test
    public void mapProcessor() {
        // Given
        final Processor p = processorFrom(Processors.map(Object::toString));
        p.init(outbox, context);
        inbox.add(1);
        inbox.add(2);

        // When
        p.process(0, inbox);
        // Then
        assertEquals(1, inbox.size());
        assertEquals(1, bucket.size());
        assertEquals("1", bucket.remove());

        // When
        p.process(0, inbox);
        // Then
        assertTrue(inbox.isEmpty());
        assertEquals(1, bucket.size());
        assertEquals("2", bucket.remove());

        // When
        p.process(0, inbox);
        // Then
        assertTrue(bucket.isEmpty());
    }

    @Test
    public void filterProcessor() {
        // Given
        final Processor p = processorFrom(Processors.filter(o -> o.equals(1)));
        p.init(outbox, context);
        inbox.add(1);
        inbox.add(2);
        inbox.add(1);
        inbox.add(2);

        // When
        p.process(0, inbox);
        // Then
        assertEquals(2, inbox.size());
        assertEquals(1, bucket.remove());

        // When
        p.process(0, inbox);
        // Then
        assertTrue(inbox.isEmpty());
        assertEquals(1, bucket.remove());

        // When
        p.process(0, inbox);
        // Then
        assertTrue(bucket.isEmpty());
    }

    @Test
    public void flatMapProcessor() {
        // Given
        final Processor p = processorFrom(Processors.flatMap(o -> traverseIterable(asList(o + "a", o + "b"))));
        p.init(outbox, context);
        inbox.add(1);

        // When
        p.process(0, inbox);
        // Then
        assertEquals(1, inbox.size());
        assertEquals(1, bucket.size());
        assertEquals("1a", bucket.remove());

        // When
        p.process(0, inbox);
        // Then
        assertTrue(inbox.isEmpty());
        assertEquals(1, bucket.size());
        assertEquals("1b", bucket.remove());

        // When
        p.process(0, inbox);
        // Then
        assertTrue(bucket.isEmpty());
    }

    @Test
    public void groupAndAccumulateFullSignature() {
        final Processor p = processorFrom(Processors.<Integer, String, List<Integer>, String>groupAndAccumulate(
                Object::toString,
                ArrayList::new,
                (list, i) -> { list.add(i); return list; },
                (i, list) -> i + ':' + list
        ));
        testGroupAndAccumulate(p, ga_stringResultTester());
    }

    @Test
    public void groupAndCollectFullSignature() {
        final Processor p = processorFrom(Processors.<Integer, String, List<Integer>, String>groupAndCollect(
                Object::toString,
                ArrayList::new,
                List::add,
                (i, list) -> i + ':' + list
        ));
        testGroupAndAccumulate(p, ga_stringResultTester());
    }

    @Test
    public void groupAndAccumulateNoFinisher() {
        final Processor p = processorFrom(Processors.<Integer, List<Integer>> groupAndAccumulate(
                Object::toString,
                ArrayList::new,
                (list, i) -> { list.add(i); return list; }
        ));
        testGroupAndAccumulate(p, ga_stringEntryResultTester());
    }

    @Test
    public void groupAndCollectNoFinisher() {
        final Processor p = processorFrom(Processors.<Integer, List<Integer>> groupAndCollect(
                Object::toString,
                ArrayList::new,
                List::add
        ));
        testGroupAndAccumulate(p, ga_stringEntryResultTester());
    }

    @Test
    public void groupAndAccumulateNoExtractorNoFinisher() {
        final Processor p = processorFrom(Processors.<Integer, List<Integer>> groupAndAccumulate(
                ArrayList::new,
                (list, i) -> { list.add(i); return list; }
        ));
        testGroupAndAccumulate(p, ga_intEntryResultTester());
    }

    @Test
    public void groupAndCollectNoExtractorNoFinisher() {
        final Processor p = processorFrom(Processors.<Integer, List<Integer>> groupAndCollect(
                ArrayList::new,
                List::add
        ));
        testGroupAndAccumulate(p, ga_intEntryResultTester());
    }

    @Test
    public void accumulateFullSignature() {
        final Processor p = processorFrom(Processors.<Integer, List<Integer>, String>accumulate(
                ArrayList::new,
                (list, i) -> { list.add(i); return list; },
                Object::toString
        ));
        testAccumulate(p, a_stringResultTester());
    }

    @Test
    public void collectFullSignature() {
        final Processor p = processorFrom(Processors.<Integer, List<Integer>, String>collect(
                ArrayList::new,
                List::add,
                Object::toString
        ));
        testAccumulate(p, a_stringResultTester());
    }

    @Test
    public void accumulateNoFinisher() {
        final Processor p = processorFrom(Processors.<Integer, List<Integer>>accumulate(
                ArrayList::new,
                (list, i) -> { list.add(i); return list; }
        ));
        testAccumulate(p, a_listResultTester());
    }

    @Test
    public void collectNoFinisher() {
        final Processor p = processorFrom(Processors.<Integer, List<Integer>>collect(
                ArrayList::new,
                List::add
        ));
        testAccumulate(p, a_listResultTester());
    }


    private static TwinConsumer<String> ga_stringResultTester() {
        final Set<String> expected = new HashSet<>(asList("1:[1, 1]", "2:[2, 2]"));
        return (String result1, String result2) -> assertEquals(expected, new HashSet<>(asList(result1, result2)));
    }

    private static TwinConsumer<Entry<String, List<Integer>>> ga_stringEntryResultTester() {
        final Set<Entry<String, List<Integer>>> expected = new HashSet<>(asList(
                entry("1", asList(1, 1)),
                entry("2", asList(2, 2))
        ));
        return (result1, result2) -> assertEquals(expected, new HashSet<>(asList(result1, result2)));
    }

    private static TwinConsumer<Entry<Integer, List<Integer>>> ga_intEntryResultTester() {
        final Set<Entry<Integer, List<Integer>>> expected = new HashSet<>(asList(
                entry(1, asList(1, 1)),
                entry(2, asList(2, 2))
        ));
        return (result1, result2) -> assertEquals(expected, new HashSet<>(asList(result1, result2)));
    }

    private static Consumer<String> a_stringResultTester() {
        return result -> assertEquals("[1, 1, 2, 2]", result);
    }

    private static Consumer<List<Integer>> a_listResultTester() {
        return result -> assertEquals(asList(1, 1, 2, 2), result);
    }

    private <R> void testGroupAndAccumulate(Processor p, TwinConsumer<R> testComplete) {
        // Given
        p.init(outbox, context);
        inbox.add(1);
        inbox.add(1);
        inbox.add(2);
        inbox.add(2);
        p.process(0, inbox);

        // When
        boolean done = p.complete();
        // Then
        assertFalse(done);
        assertEquals(1, bucket.size());
        final R result1 = (R) bucket.remove();

        // When
        done = p.complete();
        // Then
        assertTrue(done);
        assertEquals(1, bucket.size());
        final R result2 = (R) bucket.remove();

        // Finally
        testComplete.accept(result1, result2);
    }

    private <R> void testAccumulate(Processor p, Consumer<R> testComplete) {
        // Given
        p.init(outbox, context);
        inbox.add(1);
        inbox.add(1);
        inbox.add(2);
        inbox.add(2);
        p.process(0, inbox);

        // When
        final boolean done = p.complete();
        // Then
        assertTrue(done);
        assertEquals(1, bucket.size());
        final R result = (R) bucket.remove();

        // Finally
        testComplete.accept(result);
    }

    private static Processor processorFrom(ProcessorSupplier supplier) {
        return supplier.get(1).iterator().next();
    }


    private interface TwinConsumer<T> extends BiConsumer<T, T> { }
}
