package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MutableLongTest {

    @Test
    public void testToString() {
        MutableLong mutableLong = new MutableLong();
        String s = mutableLong.toString();
        assertEquals("MutableLong{value=0}", s);
    }

    @Test
    public void testEquals() {
        assertEquals(MutableLong.valueOf(0), MutableLong.valueOf(0));
        assertEquals(MutableLong.valueOf(10), MutableLong.valueOf(10));
        assertNotEquals(MutableLong.valueOf(0), MutableLong.valueOf(10));
        assertFalse(MutableLong.valueOf(0).equals(null));
        assertFalse(MutableLong.valueOf(0).equals("foo"));


        MutableLong self = MutableLong.valueOf(0);
        assertEquals(self, self);
    }

    @Test
    public void testHash() {
        assertEquals(MutableLong.valueOf(10).hashCode(), MutableLong.valueOf(10).hashCode());
    }
}
