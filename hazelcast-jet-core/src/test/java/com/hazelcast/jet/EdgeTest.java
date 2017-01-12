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

import com.hazelcast.jet.Edge.ForwardingPattern;
import com.hazelcast.jet.Processors.NoopProducer;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class EdgeTest {
    private static final String A = "a";
    private static final String B = "b";

    private Vertex a;
    private Vertex b;

    @Before
    public void before() {
        a = new Vertex(A, NoopProducer::new);
        b = new Vertex(B, NoopProducer::new);
    }

    @Test
    public void whenBetween_thenFromAndToAtOrdinalZero() {
        final Edge e = Edge.between(a, b);
        assertEquals(A, e.getSource());
        assertEquals(B, e.getDestination());
        assertEquals(0, e.getSourceOrdinal());
        assertEquals(0, e.getDestOrdinal());
    }

    @Test
    public void whenFrom_thenSourceSet() {
        final Edge e = Edge.from(a);
        assertEquals(A, e.getSource());
        assertEquals(0, e.getSourceOrdinal());
    }

    @Test
    public void whenTo_thenDestSet() {
        final Edge e = Edge.from(a).to(b);
        assertEquals(B, e.getDestination());
        assertEquals(0, e.getDestOrdinal());
    }

    @Test
    public void whenFromWithOrdinal_thenSourceAndOrdinalSet() {
        final Edge e = Edge.from(a, 1);
        assertEquals(A, e.getSource());
        assertEquals(1, e.getSourceOrdinal());
    }

    @Test
    public void whenToWithOrdinal_thenDestAndOrdinalSet() {
        final Edge e = Edge.from(a).to(b, 1);
        assertEquals(B, e.getDestination());
        assertEquals(1, e.getDestOrdinal());
    }

    @Test
    public void whenPriorityNotSet_thenDefault() {
        final Edge e = Edge.from(a);
        assertEquals(0, e.getPriority());
    }

    @Test
    public void whenPrioritySet_thenGet() {
        final Edge e = Edge.from(a).priority(2);
        assertEquals(2, e.getPriority());
    }

    @Test
    public void whenBufferedSet_thenTrue() {
        final Edge e = Edge.from(a).buffered();
        assertTrue(e.isBuffered());
    }

    @Test
    public void whenPartitionedNotSet_thenPartitionerNull() {
        final Edge e = Edge.from(a);
        assertNull(e.getPartitioner());
    }

    @Test
    public void whenPartitioned_thenPartitionerAndForwardingPatternSet() {
        // Given
        final Edge e = Edge.from(a);

        // When
        e.partitioned();
        final Partitioner partitioner = e.getPartitioner();
        assertNotNull(partitioner);
        partitioner.init(Integer.class::cast);

        // Then
        assertSame(ForwardingPattern.PARTITIONED, e.getForwardingPattern());
        assertEquals(13, partitioner.getPartition(13, 0));
    }

    @Test
    public void whenPartitionedByKey_thenPartitionerExtractsKey() {
        // Given
        final Edge e = Edge.from(a);
        final int partitioningKey = 42;

        // When
        e.partitionedByKey(o -> partitioningKey);
        final Partitioner partitioner = e.getPartitioner();
        assertNotNull(partitioner);
        partitioner.init(Integer.class::cast);

        // Then
        assertSame(ForwardingPattern.PARTITIONED, e.getForwardingPattern());
        assertEquals(partitioningKey, partitioner.getPartition(13, 0));
    }

    @Test
    public void whenPartitionedByCustom_thenCustomPartitioned() {
        // Given
        final Edge e = Edge.from(a);
        final int partitionId = 42;

        // When
        e.partitionedByCustom((o, x) -> partitionId);
        final Partitioner partitioner = e.getPartitioner();
        assertNotNull(partitioner);

        // Then
        assertSame(ForwardingPattern.PARTITIONED, e.getForwardingPattern());
        assertEquals(partitionId, partitioner.getPartition(13, 0));
    }

    @Test
    public void whenAllToOne_thenAlwaysSamePartition() {
        // Given
        final Edge e = Edge.from(a);
        final int mockPartitionCount = 100;

        // When
        e.allToOne();
        final Partitioner partitioner = e.getPartitioner();
        assertNotNull(partitioner);

        // Then
        assertSame(ForwardingPattern.PARTITIONED, e.getForwardingPattern());
        assertEquals(partitioner.getPartition(17, mockPartitionCount), partitioner.getPartition(13, mockPartitionCount));
    }

    @Test
    public void whenBroadcastSet_thenIsBroadcast() {
        final Edge e = Edge.from(a).broadcast();
        assertSame(ForwardingPattern.BROADCAST, e.getForwardingPattern());
    }

    @Test
    public void whenDistributedSet_thenIsDistributed() {
        final Edge e = Edge.from(a).distributed();
        assertTrue(e.isDistributed());
    }
}
