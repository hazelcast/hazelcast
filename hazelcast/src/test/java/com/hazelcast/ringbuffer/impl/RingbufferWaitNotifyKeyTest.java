package com.hazelcast.ringbuffer.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RingbufferWaitNotifyKeyTest {

    @Test
    public void test_equals() {
        test_equals(new RingbufferWaitNotifyKey("peter", "java"),
                new RingbufferWaitNotifyKey("peter", "java"), true);
        test_equals(new RingbufferWaitNotifyKey("peter", "java"),
                new RingbufferWaitNotifyKey("peter", "c#"), false);
        test_equals(new RingbufferWaitNotifyKey("peter", "java"),
                new RingbufferWaitNotifyKey("talip", "java"), false);
        test_equals(new RingbufferWaitNotifyKey("peter", "java"),
                new RingbufferWaitNotifyKey("talip", "c#"), false);
        test_equals(new RingbufferWaitNotifyKey("peter", "java"),
                "", false);
        test_equals(new RingbufferWaitNotifyKey("peter", "java"),
                null, false);

        RingbufferWaitNotifyKey key = new RingbufferWaitNotifyKey("peter", "java");
        test_equals(key, key, true);
    }

    public void test_equals(Object key1, Object key2, boolean equals) {
        if (equals) {
            assertEquals(key1, key2);
            // if they are equals, the hash must be equals
            assertEquals(key1.hashCode(), key2.hashCode());
        } else {
            assertNotEquals(key1, key2);
        }
    }
}
