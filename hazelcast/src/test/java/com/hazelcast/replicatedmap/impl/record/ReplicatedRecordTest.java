package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReplicatedRecordTest {

    private ReplicatedRecord<String, String> replicatedRecord;
    private ReplicatedRecord<String, String> replicatedRecordSameAttributes;

    private ReplicatedRecord<String, String> replicatedRecordOtherKey;
    private ReplicatedRecord<String, String> replicatedRecordOtherValue;
    private ReplicatedRecord<String, String> replicatedRecordOtherTtl;

    private ReplicatedRecord<String, String> tombStone;

    @Before
    public void setUp() {
        replicatedRecord = new ReplicatedRecord<String, String>("key", "value", 0);
        replicatedRecordSameAttributes = new ReplicatedRecord<String, String>("key", "value", 0);

        replicatedRecordOtherKey = new ReplicatedRecord<String, String>("otherKey", "value", 0);
        replicatedRecordOtherValue = new ReplicatedRecord<String, String>("key", "otherValue", 0);
        replicatedRecordOtherTtl = new ReplicatedRecord<String, String>("key", "value", 1);

        tombStone = new ReplicatedRecord<String, String>("key", null, 0);
    }

    @Test
    public void testGetKey() {
        assertEquals(0, replicatedRecord.getHits());
        assertEquals("key", replicatedRecord.getKey());
        assertEquals(1, replicatedRecord.getHits());
    }

    @Test
    public void testGetKeyInternal() {
        assertEquals(0, replicatedRecord.getHits());
        assertEquals("key", replicatedRecord.getKeyInternal());
        assertEquals(0, replicatedRecord.getHits());
    }

    @Test
    public void testGetValue() {
        assertEquals(0, replicatedRecord.getHits());
        assertEquals("value", replicatedRecord.getValue());
        assertEquals(1, replicatedRecord.getHits());
    }

    @Test
    public void testGetValueInternal() {
        assertEquals(0, replicatedRecord.getHits());
        assertEquals("value", replicatedRecord.getValueInternal());
        assertEquals(0, replicatedRecord.getHits());
    }

    @Test
    public void testGetTombStone() {
        assertFalse(replicatedRecord.isTombstone());
        assertTrue(tombStone.isTombstone());
    }

    @Test
    public void testGetTtlMillis() {
        assertEquals(0, replicatedRecord.getTtlMillis());
        assertEquals(1, replicatedRecordOtherTtl.getTtlMillis());
    }

    @Test
    public void testSetValue() {
        assertEquals(0, replicatedRecord.getHits());
        assertEquals("value", replicatedRecord.getValueInternal());

        replicatedRecord.setValue("newValue", 0);

        assertEquals(1, replicatedRecord.getHits());
        assertEquals("newValue", replicatedRecord.getValueInternal());
    }

    @Test
    public void testSetValueInternal() {
        assertEquals(0, replicatedRecord.getHits());
        assertEquals("value", replicatedRecord.getValueInternal());

        replicatedRecord.setValueInternal("newValue", 0);

        assertEquals(0, replicatedRecord.getHits());
        assertEquals("newValue", replicatedRecord.getValueInternal());
    }

    @Test
    public void testGetUpdateTime() {
        long lastUpdateTime = replicatedRecord.getUpdateTime();
        sleepMillis(100);

        replicatedRecord.setValue("newValue", 0);
        assertTrue("replicatedRecord.getUpdateTime() should return a greater update time",
                replicatedRecord.getUpdateTime() > lastUpdateTime);
    }

    @Test
    public void testSetUpdateTime() {
        replicatedRecord.setUpdateTime(2342);
        assertEquals(2342, replicatedRecord.getUpdateTime());
    }

    @Test
    public void testSetHits() {
        replicatedRecord.setHits(4223);
        assertEquals(4223, replicatedRecord.getHits());
    }

    @Test
    public void getLastAccessTime() {
        long lastAccessTime = replicatedRecord.getLastAccessTime();
        sleepMillis(100);

        replicatedRecord.setValue("newValue", 0);
        assertTrue("replicatedRecord.getLastAccessTime() should return a greater access time",
                replicatedRecord.getLastAccessTime() > lastAccessTime);
    }

    @Test
    public void testSetAccessTime() {
        replicatedRecord.setLastAccessTime(1234);
        assertEquals(1234, replicatedRecord.getLastAccessTime());
    }

    @Test
    public void testCreationTime() {
        replicatedRecord.setCreationTime(4321);
        assertEquals(4321, replicatedRecord.getCreationTime());
    }

    @Test
    public void testEquals() {
        assertEquals(replicatedRecord, replicatedRecord);
        assertEquals(replicatedRecord, replicatedRecordSameAttributes);

        assertNotEquals(replicatedRecord, null);
        assertNotEquals(replicatedRecord, new Object());

        assertNotEquals(replicatedRecord, replicatedRecordOtherKey);
        assertNotEquals(replicatedRecord, replicatedRecordOtherValue);
        assertNotEquals(replicatedRecord, replicatedRecordOtherTtl);
    }

    @Test
    public void testHashCode() {
        assertEquals(replicatedRecord.hashCode(), replicatedRecord.hashCode());
        assertEquals(replicatedRecord.hashCode(), replicatedRecordSameAttributes.hashCode());

        assertNotEquals(replicatedRecord.hashCode(), replicatedRecordOtherKey.hashCode());
        assertNotEquals(replicatedRecord.hashCode(), replicatedRecordOtherValue.hashCode());
        assertNotEquals(replicatedRecord.hashCode(), replicatedRecordOtherTtl.hashCode());
    }

    @Test
    public void testToString() {
        assertNotNull(replicatedRecord.toString());
    }
}
