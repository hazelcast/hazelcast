package com.hazelcast.ringbuffer.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RingbufferMigrationTest extends HazelcastTestSupport {

    public static final int CAPACITY = 100;
    private TestHazelcastInstanceFactory instanceFactory;

    @Before
    public void setup() {
        instanceFactory = createHazelcastInstanceFactory(3);
    }

    @Test
    public void test() throws Exception{
        HazelcastInstance hz1 = instanceFactory.newHazelcastInstance();

        for (int k = 0; k < 10 * CAPACITY; k++) {
            hz1.getRingbuffer("ringbuffer").add(k);
        }

        long oldTailSeq = hz1.getRingbuffer("ringbuffer").tailSequence();
        long oldHeadSeq = hz1.getRingbuffer("ringbuffer").headSequence();

        HazelcastInstance hz2 = instanceFactory.newHazelcastInstance();
        HazelcastInstance hz3 = instanceFactory.newHazelcastInstance();
        hz1.shutdown();

        sleepSeconds(5);

        assertEquals(oldTailSeq, hz2.getRingbuffer("ringbuffer").tailSequence());
        assertEquals(oldHeadSeq, hz2.getRingbuffer("ringbuffer").headSequence());
    }
}
