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
import com.hazelcast.query.TestUtil;
import com.hazelcast.query.MapIndex;
import org.junit.Test;

import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.*;

public class MapIndexTest extends TestUtil {

    @Test
    public void testBasics() {
        testIt(true);
        testIt(false);
    }

    private void testIt(boolean ordered) {
        MapIndex mapIndex = new MapIndex(null, ordered, 0);
        assertEquals(0, mapIndex.getRecords(0).size());
        assertEquals(0, mapIndex.getSubRecordsBetween(0, 1000).size());
        Record record5 = newRecord(5L);
        assertEquals(5L, record5.getId());
        mapIndex.index(55, record5);
        assertEquals(1, mapIndex.getRecordValues().size());
        assertEquals(new Long(55L), mapIndex.getRecordValues().get(5L));
        ConcurrentMap<Long, Record> records = mapIndex.getMapRecords().get(55L);
        assertNotNull(records);
        assertEquals(record5, records.get(5L));
        Record record6 = newRecord(6L);
        assertEquals(6L, record6.getId());
        mapIndex.index(66, record6);
        assertEquals(2, mapIndex.getRecordValues().size());
        assertEquals(new Long(66L), mapIndex.getRecordValues().get(6L));
        records = mapIndex.getMapRecords().get(66L);
        assertNotNull(records);
        assertEquals(record6, records.get(6L));
        mapIndex.index(555, record5);
        assertEquals(2, mapIndex.getRecordValues().size());
        assertEquals(2, mapIndex.getMapRecords().size());
        assertEquals(new Long(555L), mapIndex.getRecordValues().get(5L));
        records = mapIndex.getMapRecords().get(555L);
        assertNotNull(records);
        assertEquals(record5, records.get(5L));
        assertEquals(1, mapIndex.getRecords(555).size());
        assertEquals(2, mapIndex.getSubRecordsBetween(55, 555).size());
        assertEquals(2, mapIndex.getSubRecordsBetween(66, 555).size());
        assertEquals(1, mapIndex.getSubRecordsBetween(555, 555).size());
        Record record50 = newRecord(50);
        mapIndex.index(555, record50);
        assertEquals(3, mapIndex.getRecordValues().size());
        assertEquals(2, mapIndex.getMapRecords().size());
        assertEquals(new Long(555L), mapIndex.getRecordValues().get(5L));
        assertEquals(new Long(555L), mapIndex.getRecordValues().get(50L));
        records = mapIndex.getMapRecords().get(555L);
        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(record5, records.get(5L));
        assertEquals(record50, records.get(50L));
        assertEquals(2, mapIndex.getRecords(555).size());
        assertEquals(3, mapIndex.getSubRecordsBetween(55, 555).size());
        assertEquals(3, mapIndex.getSubRecordsBetween(66, 555).size());
        assertEquals(2, mapIndex.getSubRecordsBetween(555, 555).size());
        assertEquals(0, mapIndex.getSubRecords(false, true, 66).size());
        assertEquals(1, mapIndex.getSubRecords(true, true, 66).size());
        assertEquals(1, mapIndex.getSubRecords(true, true, 67).size());
        assertEquals(2, mapIndex.getSubRecords(false, false, 66).size());
        assertEquals(3, mapIndex.getSubRecords(true, false, 66).size());
        assertEquals(3, mapIndex.getSubRecords(true, false, 61).size());
        assertEquals(3, mapIndex.getRecords(new long[] {66, 555, 34234}).size());
        assertEquals(2, mapIndex.getRecords(new long[] {555, 34234}).size());
        record5.setActive(false);
        mapIndex.index(-1, record5);
        assertEquals(2, mapIndex.getRecordValues().size());
        assertEquals(2, mapIndex.getMapRecords().size());
        assertEquals(new Long(555L), mapIndex.getRecordValues().get(50L));
        assertEquals(null, mapIndex.getRecordValues().get(5L));
        records = mapIndex.getMapRecords().get(555L);
        assertNotNull(records);
        assertEquals(null, records.get(5L));
        assertEquals(record50, records.get(50L));
        assertEquals(1, mapIndex.getRecords(555).size());
        assertEquals(2, mapIndex.getSubRecordsBetween(55, 555).size());
        assertEquals(2, mapIndex.getSubRecordsBetween(66, 555).size());
        assertEquals(1, mapIndex.getSubRecordsBetween(555, 555).size());
        assertEquals(0, mapIndex.getSubRecords(false, true, 66).size());
        assertEquals(1, mapIndex.getSubRecords(true, true, 66).size());
        assertEquals(1, mapIndex.getSubRecords(true, true, 67).size());
        assertEquals(1, mapIndex.getSubRecords(false, false, 66).size());
        assertEquals(2, mapIndex.getSubRecords(true, false, 66).size());
        assertEquals(2, mapIndex.getSubRecords(true, false, 61).size());
        record50.setActive(false);
        mapIndex.index(-1, record50);
        assertEquals(1, mapIndex.getRecordValues().size());
        assertEquals(1, mapIndex.getMapRecords().size());
        assertEquals(null, mapIndex.getRecordValues().get(50L));
        records = mapIndex.getMapRecords().get(555L);
        assertNull(records);
        assertEquals(0, mapIndex.getRecords(555).size());
        assertEquals(1, mapIndex.getSubRecordsBetween(55, 555).size());
        assertEquals(1, mapIndex.getSubRecordsBetween(66, 555).size());
        assertEquals(0, mapIndex.getSubRecordsBetween(555, 555).size());
        record6.setActive(false);
        mapIndex.index(-1, record6);
        assertEquals(0, mapIndex.getRecordValues().size());
        assertEquals(0, mapIndex.getMapRecords().size());
        assertEquals(null, mapIndex.getRecordValues().get(6L));
        assertNull(mapIndex.getMapRecords().get(66L));
        assertEquals(0, mapIndex.getRecords(555).size());
        assertEquals(0, mapIndex.getSubRecordsBetween(55, 555).size());
        assertEquals(0, mapIndex.getSubRecordsBetween(66, 555).size());
        assertEquals(0, mapIndex.getSubRecordsBetween(555, 555).size());
    }


}
