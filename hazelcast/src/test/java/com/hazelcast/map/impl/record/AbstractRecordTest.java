/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
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
@Category({QuickTest.class, ParallelTest.class})
public class AbstractRecordTest {

    private static final Data KEY = mock(Data.class);
    private static final Object VALUE = new Object();

    private ObjectRecord record;
    private ObjectRecord recordSameAttributes;
    private ObjectRecord recordOtherVersion;
    private ObjectRecord recordOtherTtl;
    private ObjectRecord recordOtherCreationTime;
    private ObjectRecord recordOtherHits;
    private ObjectRecord recordOtherLastAccessTime;
    private ObjectRecord recordOtherLastUpdateTime;

    @Before
    public void setUp() throws Exception {
        record = new ObjectRecord(VALUE);
        record.setKey(KEY);

        recordSameAttributes = new ObjectRecord();
        recordSameAttributes.setKey(KEY);
        recordSameAttributes.setValue(VALUE);

        recordOtherVersion = new ObjectRecord(VALUE);
        recordOtherVersion.setKey(KEY);
        recordOtherVersion.setVersion(42);

        recordOtherTtl = new ObjectRecord(VALUE);
        recordOtherTtl.setKey(KEY);
        recordOtherTtl.setTtl(2342);

        recordOtherCreationTime = new ObjectRecord(VALUE);
        recordOtherCreationTime.setKey(KEY);
        recordOtherCreationTime.setCreationTime(152344145);

        recordOtherHits = new ObjectRecord(VALUE);
        recordOtherHits.setKey(KEY);
        recordOtherHits.setHits(23);

        recordOtherLastAccessTime = new ObjectRecord(VALUE);
        recordOtherLastAccessTime.setKey(KEY);
        recordOtherLastAccessTime.setLastAccessTime(354712354);

        recordOtherLastUpdateTime = new ObjectRecord(VALUE);
        recordOtherLastUpdateTime.setKey(KEY);
        recordOtherLastUpdateTime.setLastUpdateTime(124241425);
    }

    @Test
    public void testCasCachedValue() {
        assertTrue(record.casCachedValue(null, null));
    }

    @Test
    public void testSetSequence_doesNothing() {
        assertEquals(AbstractRecord.NOT_AVAILABLE, record.getSequence());

        record.setSequence(1250293);

        assertEquals(AbstractRecord.NOT_AVAILABLE, record.getSequence());
    }

    @Test
    public void testEquals() {
        assertEquals(record, record);
        assertEquals(record, recordSameAttributes);

        assertNotEquals(record, null);
        assertNotEquals(record, new Object());

        assertNotEquals(record, recordOtherVersion);
        assertNotEquals(record, recordOtherTtl);
        assertNotEquals(record, recordOtherCreationTime);
        assertNotEquals(record, recordOtherHits);
        assertNotEquals(record, recordOtherLastAccessTime);
        assertNotEquals(record, recordOtherLastUpdateTime);
    }

    @Test
    public void testHashCode() {
        assertEquals(record.hashCode(), record.hashCode());
        assertEquals(record.hashCode(), recordSameAttributes.hashCode());

        assumeDifferentHashCodes();
        assertNotEquals(record.hashCode(), recordOtherVersion.hashCode());
        assertNotEquals(record.hashCode(), recordOtherTtl.hashCode());
        assertNotEquals(record.hashCode(), recordOtherCreationTime.hashCode());
        assertNotEquals(record.hashCode(), recordOtherHits.hashCode());
        assertNotEquals(record.hashCode(), recordOtherLastAccessTime.hashCode());
        assertNotEquals(record.hashCode(), recordOtherLastUpdateTime.hashCode());
    }
}
