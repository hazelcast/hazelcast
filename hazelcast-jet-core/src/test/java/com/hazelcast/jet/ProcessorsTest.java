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
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.AggregateOperationImpl;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ProcessorsTest {
    private ArrayDequeInbox inbox;
    private ArrayDequeOutbox outbox;
    private Queue<Object> bucket;
    private Context context;

    @Before
    public void before() {
        inbox = new ArrayDequeInbox();
        outbox = new ArrayDequeOutbox(new int[]{1}, new ProgressTracker());
        context = mock(Context.class);
        bucket = outbox.queueWithOrdinal(0);
    }

    @Test
    public void map() {
        // Given
        final Processor p = processorFrom(Processors.map(Object::toString));
        inbox.add(1);

        // When
        p.process(0, inbox);

        // Then
        assertEquals("1", bucket.remove());
    }

    @Test
    public void filter() {
        // Given
        final Processor p = processorFrom(Processors.filter(o -> o.equals(1)));
        inbox.add(1);
        inbox.add(2);
        inbox.add(1);
        inbox.add(2);

        // When
        p.process(0, inbox);
        // Then
        assertEquals(1, bucket.remove());

        // When
        p.process(0, inbox);
        // Then
        assertTrue(inbox.isEmpty());
        assertEquals(1, bucket.remove());
    }

    @Test
    public void flatMap() {
        // Given
        final Processor p = processorFrom(Processors.flatMap(o -> traverseIterable(asList(o + "a", o + "b"))));
        inbox.add(1);

        // When
        p.process(0, inbox);
        // Then
        assertEquals("1a", bucket.remove());

        // When
        p.process(0, inbox);
        // Then
        assertTrue(inbox.isEmpty());
        assertEquals("1b", bucket.remove());
    }

    @Test
    public void groupAndAggregate() {
        final Processor p = processorFrom(Processors.aggregateByKey(Object::toString, aggregateToList()));
        // Given
        inbox.add(1);
        inbox.add(1);
        inbox.add(2);
        inbox.add(2);
        p.process(0, inbox);

        // When
        boolean done = p.complete();
        // Then
        assertFalse(done);
        final Entry<String, List<Integer>> result1 = (Entry<String, List<Integer>>) bucket.remove();

        // When
        done = p.complete();
        // Then
        assertTrue(done);
        final Entry<String, List<Integer>> result2 = (Entry<String, List<Integer>>) bucket.remove();

        // Finally
        ga_stringEntryResultTester().accept(result1, result2);
    }

    private static TwinConsumer<Entry<String, List<Integer>>> ga_stringEntryResultTester() {
        final Set<Entry<String, List<Integer>>> expected = new HashSet<>(asList(
                entry("1", asList(1, 1)),
                entry("2", asList(2, 2))
        ));
        return (result1, result2) -> assertEquals(expected, new HashSet<>(asList(result1, result2)));
    }

    private Processor processorFrom(Supplier<Processor> supplier) {
        Processor p = supplier.get();
        p.init(outbox, context);
        return p;
    }

    private static <T> AggregateOperation<T, List<T>, List<T>> aggregateToList() {
        return new AggregateOperationImpl<>(
                ArrayList::new,
                List::add,
                List::addAll,
                null,
                DistributedFunction.identity()
        );
    }

    private interface TwinConsumer<T> extends BiConsumer<T, T> { }
}
