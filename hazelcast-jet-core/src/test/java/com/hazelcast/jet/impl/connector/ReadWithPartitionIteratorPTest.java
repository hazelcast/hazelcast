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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ReadWithPartitionIteratorPTest {

    @Test
    @SuppressWarnings("unchecked")
    public void when_readFromTwoPartitions_then_emitRoundRobin() {

        // Given
        final List<Integer> partitions = asList(0, 1);
        final Iterator<Entry<Integer, Integer>>[] content = new Iterator[]{
                iterate(51, 52, 53),
                iterate(71, 72, 73),
        };
        ReadWithPartitionIteratorP<Entry<Integer, Integer>> r =
                new ReadWithPartitionIteratorP<>(p -> content[p], partitions);
        TestOutbox outbox = new TestOutbox(3);
        Queue<Object> bucket = outbox.queueWithOrdinal(0);
        r.init(outbox, mock(Processor.Context.class));

        // When
        assertFalse(r.complete());

        // Then
        assertEquals(entry(51), bucket.poll());
        assertEquals(entry(71), bucket.poll());
        assertEquals(entry(52), bucket.poll());

        // When
        assertTrue(r.complete());

        // Then
        assertEquals(entry(72), bucket.poll());
        assertEquals(entry(53), bucket.poll());
        assertEquals(entry(73), bucket.poll());
    }

    private static Iterator<Entry<Integer, Integer>> iterate(Integer... content) {
        return Stream.of(content).map(ReadWithPartitionIteratorPTest::entry).iterator();
    }

    private static Entry<Integer, Integer> entry(Integer content) {
        return new SimpleImmutableEntry<>(content, content);
    }
}
