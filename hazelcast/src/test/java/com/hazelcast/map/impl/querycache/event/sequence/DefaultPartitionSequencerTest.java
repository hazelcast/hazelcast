package com.hazelcast.map.impl.querycache.event.sequence;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DefaultPartitionSequencerTest {
    
    private DefaultPartitionSequencer sequencer = new DefaultPartitionSequencer();
    
    @Test
    public void testNextSequence() {
        assertEquals(1, sequencer.nextSequence());
        assertEquals(2, sequencer.nextSequence());
    }

    @Test
    public void testSetSequence() {
        sequencer.setSequence(23);

        assertEquals(23, sequencer.getSequence());
    }

    @Test
    public void testCompareAndSetSequence() {
        sequencer.compareAndSetSequence(23, 42);
        assertEquals(0, sequencer.getSequence());

        sequencer.compareAndSetSequence(0, 42);
        assertEquals(42, sequencer.getSequence());
    }

    @Test
    public void testGetSequence() {
        assertEquals(0, sequencer.getSequence());
    }

    @Test
    public void testReset() {
        sequencer.setSequence(42);
        sequencer.reset();

        assertEquals(0, sequencer.getSequence());
    }
}
