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

package com.hazelcast.map.impl.record;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataRecordWithStatsTest {

    private static final Data VALUE = mock(Data.class);

    private DataRecordWithStats record;
    private DataRecordWithStats recordSameAttributes;
    private DataRecordWithStats recordOtherKeyAndValue;
    private ObjectRecordWithStats objectRecord;

    @Before
    public void setUp() {
        Data key = mock(Data.class);
        Data otherKey = mock(Data.class);

        record = new DataRecordWithStats(VALUE);

        recordSameAttributes = new DataRecordWithStats();
        recordSameAttributes.setValue(VALUE);

        recordOtherKeyAndValue = new DataRecordWithStats();
        recordOtherKeyAndValue.setValue(otherKey);

        objectRecord = new ObjectRecordWithStats();
        objectRecord.setValue(new Object());
    }

    @Test
    public void testGetValue() {
        assertEquals(VALUE, record.getValue());
        assertEquals(VALUE, recordSameAttributes.getValue());
        assertNotEquals(VALUE, recordOtherKeyAndValue.getValue());
    }

    @Test
    public void testGetCosts() {
        assertTrue(record.getCost() > 0);
        assertTrue(recordSameAttributes.getCost() > 0);
        assertTrue(recordOtherKeyAndValue.getCost() > 0);
    }

    @Test
    public void testEquals() {
        assertEquals(record, record);
        assertEquals(record, recordSameAttributes);

        assertNotEquals(record, null);
        assertNotEquals(record, new Object());

        assertNotEquals(record, objectRecord);
        assertNotEquals(record, recordOtherKeyAndValue);
    }

    @Test
    public void testHashCode() {
        assertEquals(record.hashCode(), record.hashCode());
        assertEquals(record.hashCode(), recordSameAttributes.hashCode());

        assumeDifferentHashCodes();
        assertNotEquals(record.hashCode(), objectRecord.hashCode());
        assertNotEquals(record.hashCode(), recordOtherKeyAndValue.hashCode());
    }
}
