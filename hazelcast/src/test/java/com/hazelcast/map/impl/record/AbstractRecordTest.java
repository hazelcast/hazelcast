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

import com.hazelcast.internal.util.Clock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.record.Record.UNSET;
import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractRecordTest {

    private static final Object VALUE = new Object();

    private Record record;
    private Record recordSameAttributes;
    private Record recordOtherVersion;
    private Record recordOtherCreationTime;
    private Record recordOtherHits;
    private Record recordOtherLastAccessTime;
    private Record recordOtherLastUpdateTime;

    @Before
    public void setUp() throws Exception {
        record = new ObjectRecordWithStats(VALUE);

        recordSameAttributes = new ObjectRecordWithStats();
        recordSameAttributes.setValue(VALUE);

        recordOtherVersion = new ObjectRecordWithStats(VALUE);
        recordOtherVersion.setVersion(42);

        recordOtherCreationTime = new ObjectRecordWithStats(VALUE);
        recordOtherCreationTime.setCreationTime(Clock.currentTimeMillis());

        recordOtherHits = new ObjectRecordWithStats(VALUE);
        recordOtherHits.setHits(23);

        recordOtherLastAccessTime = new ObjectRecordWithStats(VALUE);
        recordOtherLastAccessTime.setLastAccessTime(Clock.currentTimeMillis());

        recordOtherLastUpdateTime = new ObjectRecordWithStats(VALUE);
        recordOtherLastUpdateTime.setLastUpdateTime(Clock.currentTimeMillis() + 10000);
    }

    @Test
    public void testGetCachedValueUnsafe() {
        assertEquals(Record.NOT_CACHED, record.getCachedValueUnsafe());
    }

    @Test
    public void testSetSequence_doesNothing() {
        assertEquals(UNSET, record.getSequence());

        record.setSequence(1250293);

        assertEquals(UNSET, record.getSequence());
    }

    @Test
    public void testEquals() {
        assertEquals(record, record);
        assertEquals(record, recordSameAttributes);

        assertNotEquals(record, null);
        assertNotEquals(record, new Object());

        assertNotEquals(record, recordOtherVersion);
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
        assertNotEquals(record.hashCode(), recordOtherCreationTime.hashCode());
        assertNotEquals(record.hashCode(), recordOtherHits.hashCode());
        assertNotEquals(record.hashCode(), recordOtherLastAccessTime.hashCode());
        assertNotEquals(record.hashCode(), recordOtherLastUpdateTime.hashCode());
    }
}
