/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedRecordTest {

    private ReplicatedRecord<String, String> replicatedRecord;
    private ReplicatedRecord<String, String> replicatedRecordSameAttributes;

    private ReplicatedRecord<String, String> replicatedRecordOtherKey;
    private ReplicatedRecord<String, String> replicatedRecordOtherValue;
    private ReplicatedRecord<String, String> replicatedRecordOtherTtl;

    @Before
    public void setUp() {
        replicatedRecord = new ReplicatedRecord<String, String>("key", "value", 0);
        replicatedRecordSameAttributes = new ReplicatedRecord<String, String>("key", "value", 0);

        replicatedRecordOtherKey = new ReplicatedRecord<String, String>("otherKey", "value", 0);
        replicatedRecordOtherValue = new ReplicatedRecord<String, String>("key", "otherValue", 0);
        replicatedRecordOtherTtl = new ReplicatedRecord<String, String>("key", "value", 1);
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
        sleepAtLeastMillis(100);

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
        sleepAtLeastMillis(100);

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

        assumeDifferentHashCodes();
        assertNotEquals(replicatedRecord.hashCode(), replicatedRecordOtherKey.hashCode());
        assertNotEquals(replicatedRecord.hashCode(), replicatedRecordOtherValue.hashCode());
        assertNotEquals(replicatedRecord.hashCode(), replicatedRecordOtherTtl.hashCode());
    }

    @Test
    public void testToString() {
        assertNotNull(replicatedRecord.toString());
    }
}
