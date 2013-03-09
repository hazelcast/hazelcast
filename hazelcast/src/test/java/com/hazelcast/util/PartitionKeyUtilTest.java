package com.hazelcast.util;

import org.junit.Test;

import java.util.Date;

import static com.hazelcast.util.PartitionKeyUtil.getBaseName;
import static com.hazelcast.util.PartitionKeyUtil.getPartitionKey;
import static org.junit.Assert.assertEquals;

public class PartitionKeyUtilTest {

    @Test
    public void testGetBaseName() {
        assertEquals("foo", getBaseName("foo"));
        assertEquals("", getBaseName(""));
        assertEquals(null, getBaseName(null));
        assertEquals("foo", getBaseName("foo@bar"));
        assertEquals("foo", getBaseName("foo@"));
        assertEquals("", getBaseName("@bar"));
        assertEquals("foo", getBaseName("foo@bar@nii"));
    }

    @Test
    public void testGetPartitionKey() {
        Date date = new Date();
        assertEquals(date, getPartitionKey(date));
        assertEquals("foo", getPartitionKey("foo"));
        assertEquals("", getPartitionKey(""));
        assertEquals(null, getPartitionKey(null));
        assertEquals("bar", getPartitionKey("foo@bar"));
        assertEquals("", getPartitionKey("foo@"));
        assertEquals("bar", getPartitionKey("@bar"));
        assertEquals("bar@nii", getPartitionKey("foo@bar@nii"));
    }
}
