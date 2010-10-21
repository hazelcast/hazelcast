/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.query;

import com.hazelcast.impl.Record;
import com.hazelcast.impl.TestUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.*;

public class IndexTest extends TestUtil {

    @Test
    public void testBasics() {
        testIt(true);
        testIt(false);
    }

    private void testIt(boolean ordered) {
        Index index = new Index(null, ordered, 0);
        assertEquals(0, index.getRecords(0L).size());
        assertEquals(0, index.getSubRecordsBetween(0L, 1000L).size());
        Record record5 = newRecord(5L);
        assertEquals(5L, (Object) record5.getId());
        index.index(55L, record5);
        assertEquals(1, index.getRecordValues().size());
        assertEquals(new Long(55L), index.getRecordValues().get(5L));
        ConcurrentMap<Long, Record> records = index.getMapRecords().get(55L);
        assertNotNull(records);
        assertEquals(record5, records.get(5L));
        Record record6 = newRecord(6L);
        assertEquals(6L, (Object) record6.getId());
        index.index(66L, record6);
        assertEquals(2, index.getRecordValues().size());
        assertEquals(new Long(66L), index.getRecordValues().get(6L));
        records = index.getMapRecords().get(66L);
        assertNotNull(records);
        assertEquals(record6, records.get(6L));
        index.index(555L, record5);
        assertEquals(2, index.getRecordValues().size());
        assertEquals(2, index.getMapRecords().size());
        assertEquals(new Long(555L), index.getRecordValues().get(5L));
        records = index.getMapRecords().get(555L);
        assertNotNull(records);
        assertEquals(record5, records.get(5L));
        assertEquals(1, index.getRecords(555L).size());
        assertEquals(2, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(2, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(1, index.getSubRecordsBetween(555L, 555L).size());
        Record record50 = newRecord(50);
        index.index(555L, record50);
        assertEquals(3, index.getRecordValues().size());
        assertEquals(2, index.getMapRecords().size());
        assertEquals(new Long(555L), index.getRecordValues().get(5L));
        assertEquals(new Long(555L), index.getRecordValues().get(50L));
        records = index.getMapRecords().get(555L);
        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(record5, records.get(5L));
        assertEquals(record50, records.get(50L));
        assertEquals(2, index.getRecords(555L).size());
        assertEquals(3, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(3, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(2, index.getSubRecordsBetween(555L, 555L).size());
        assertEquals(0, index.getSubRecords(false, true, 66L).size());
        assertEquals(1, index.getSubRecords(true, true, 66L).size());
        assertEquals(1, index.getSubRecords(true, true, 67L).size());
        assertEquals(2, index.getSubRecords(false, false, 66L).size());
        assertEquals(3, index.getSubRecords(true, false, 66L).size());
        assertEquals(3, index.getSubRecords(true, false, 61L).size());
        assertEquals(3, index.getRecords(new HashSet<Long>(Arrays.asList(66L, 555L, 34234L))).size());
        assertEquals(2, index.getRecords(new HashSet<Long>(Arrays.asList(555L, 34234L))).size());
        record5.setActive(false);
        index.index(-1L, record5);
        assertEquals(2, index.getRecordValues().size());
        assertEquals(2, index.getMapRecords().size());
        assertEquals(new Long(555L), index.getRecordValues().get(50L));
        assertEquals(null, index.getRecordValues().get(5L));
        records = index.getMapRecords().get(555L);
        assertNotNull(records);
        assertEquals(null, records.get(5L));
        assertEquals(record50, records.get(50L));
        assertEquals(1, index.getRecords(555L).size());
        assertEquals(2, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(2, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(1, index.getSubRecordsBetween(555L, 555L).size());
        assertEquals(0, index.getSubRecords(false, true, 66L).size());
        assertEquals(1, index.getSubRecords(true, true, 66L).size());
        assertEquals(1, index.getSubRecords(true, true, 67L).size());
        assertEquals(1, index.getSubRecords(false, false, 66L).size());
        assertEquals(2, index.getSubRecords(true, false, 66L).size());
        assertEquals(2, index.getSubRecords(true, false, 61L).size());
        record50.setActive(false);
        index.index(-1L, record50);
        assertEquals(1, index.getRecordValues().size());
        assertEquals(1, index.getMapRecords().size());
        assertEquals(null, index.getRecordValues().get(50L));
        records = index.getMapRecords().get(555L);
        assertNull(records);
        assertEquals(0, index.getRecords(555L).size());
        assertEquals(1, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(1, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(0, index.getSubRecordsBetween(555L, 555L).size());
        record6.setActive(false);
        index.index(-1L, record6);
        assertEquals(0, index.getRecordValues().size());
        assertEquals(0, index.getMapRecords().size());
        assertEquals(null, index.getRecordValues().get(6L));
        assertNull(index.getMapRecords().get(66L));
        assertEquals(0, index.getRecords(555L).size());
        assertEquals(0, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(0, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(0, index.getSubRecordsBetween(555L, 555L).size());
    }
}
