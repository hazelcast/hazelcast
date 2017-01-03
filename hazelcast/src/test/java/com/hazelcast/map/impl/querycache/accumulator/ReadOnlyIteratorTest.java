package com.hazelcast.map.impl.querycache.accumulator;

import com.hazelcast.map.impl.querycache.accumulator.BasicAccumulator.ReadOnlyIterator;
import com.hazelcast.map.impl.querycache.event.DefaultQueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReadOnlyIteratorTest {

    private Sequenced sequenced = new DefaultQueryCacheEventData();

    private ReadOnlyIterator<Sequenced> iterator;

    @Before
    public void setUp() {
        CyclicBuffer<Sequenced> buffer = new DefaultCyclicBuffer<Sequenced>(1);
        buffer.add(sequenced);

        iterator = new ReadOnlyIterator<Sequenced>(buffer);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_whenBufferIsNull_thenThrowException() {
        iterator = new ReadOnlyIterator<Sequenced>(null);
    }

    @Test
    public void testIteration() {
        assertTrue(iterator.hasNext());
        assertEquals(sequenced, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        iterator.remove();
    }
}
