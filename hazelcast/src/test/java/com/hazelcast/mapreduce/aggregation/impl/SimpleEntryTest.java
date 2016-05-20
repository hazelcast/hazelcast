package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SimpleEntryTest {

    private SimpleEntry<String, String> simpleEntry = new SimpleEntry<String, String>();

    @Test
    public void testGetKey() {
        assertNull(simpleEntry.getKey());
    }

    @Test
    public void testGetValue() {
        assertNull(simpleEntry.getValue());
    }

    @Test
    public void testGetTargetObject() {
        simpleEntry.setKey("targetKey");
        simpleEntry.setValue("targetValue");

        assertEquals("targetKey", simpleEntry.getTargetObject(true));
        assertEquals("targetValue", simpleEntry.getTargetObject(false));
    }

    @Test
    public void testSetKey() {
        simpleEntry.setKey("key");

        assertEquals("key", simpleEntry.getKey());
    }

    @Test
    public void testSetValue() {
        simpleEntry.setValue("value");

        assertEquals("value", simpleEntry.getValue());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetKeyData() {
        simpleEntry.getKeyData();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetValueData() {
        simpleEntry.getValueData();
    }
}
