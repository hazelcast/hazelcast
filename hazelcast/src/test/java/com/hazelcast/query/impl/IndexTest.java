/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.TestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import static junit.framework.Assert.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class IndexTest extends TestUtil {

    @Test
    public void testBasics() {
        testIt(true);
        testIt(false);
    }

    QueryRecord newRecord(Object key, final Comparable attributeValue) {
        return new QueryRecord(toData(key), attributeValue);
    }

    class QueryRecord implements QueryableEntry {
        Data key;
        Comparable attributeValue;

        QueryRecord(Data key, Comparable attributeValue) {
            this.key = key;
            this.attributeValue = attributeValue;
        }

        public Comparable getAttribute(String attributeName) throws QueryException {
            return attributeValue;
        }

        public AttributeType getAttributeType(String attributeName) {
            return ReflectionHelper.getAttributeType(attributeValue.getClass());
        }

        public Data getKeyData() {
            return key;
        }

        public Data getValueData() {
            return null;
        }

        public Data getIndexKey() {
            return key;
        }

        public long getCreationTime() {
            return 0;
        }

        public long getLastAccessTime() {
            return 0;
        }

        public Object getKey() {
            return key;
        }

        public Object getValue() {
            return null;
        }

        public Object setValue(Object value) {
            return null;
        }

        public void changeAttribute(Comparable newAttributeValue) {
            this.attributeValue = newAttributeValue;
        }

        public void writeData(ObjectDataOutput out) throws IOException {
        }

        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    private void testIt(boolean ordered) {
        IndexImpl index = new IndexImpl(null, ordered);
        assertEquals(0, index.getRecords(0L).size());
        assertEquals(0, index.getSubRecordsBetween(0L, 1000L).size());
        QueryRecord record5 = newRecord(5L, 55L);
        index.saveEntryIndex(record5);
        assertEquals(1, index.getRecordValues().size());
        assertEquals(55L, index.getRecordValues().get(record5.getIndexKey()));
        QueryRecord record6 = newRecord(6L, 66L);
        index.saveEntryIndex(record6);
        assertEquals(2, index.getRecordValues().size());
        assertEquals(new Long(66L), index.getRecordValues().get(record6.getIndexKey()));
        record5.changeAttribute(555L);
        index.saveEntryIndex(record5);
        assertEquals(2, index.getRecordValues().size());
        assertEquals(new Long(555L), index.getRecordValues().get(record5.getIndexKey()));
        assertEquals(1, index.getRecords(555L).size());
        assertEquals(2, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(2, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(1, index.getSubRecordsBetween(555L, 555L).size());
        QueryRecord record50 = newRecord(50L, 555L);
        index.saveEntryIndex(record50);
        assertEquals(3, index.getRecordValues().size());
        assertEquals(new Long(555L), index.getRecordValues().get(record5.getIndexKey()));
        assertEquals(new Long(555L), index.getRecordValues().get(record50.getIndexKey()));
        ConcurrentMap<Data, QueryableEntry> records = index.getRecordMap(555L);
        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(record5, records.get(record5.getIndexKey()));
        assertEquals(record50, records.get(record50.getIndexKey()));
        assertEquals(2, index.getRecords(555L).size());
        assertEquals(3, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(3, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(2, index.getSubRecordsBetween(555L, 555L).size());
        assertEquals(0, index.getSubRecords(ComparisonType.LESSER, 66L).size());
        assertEquals(1, index.getSubRecords(ComparisonType.LESSER_EQUAL, 66L).size());
        assertEquals(1, index.getSubRecords(ComparisonType.LESSER_EQUAL, 67L).size());
        assertEquals(2, index.getSubRecords(ComparisonType.GREATER, 66L).size());
        assertEquals(3, index.getSubRecords(ComparisonType.GREATER_EQUAL, 66L).size());
        assertEquals(3, index.getSubRecords(ComparisonType.GREATER_EQUAL, 61L).size());
        assertEquals(3, index.getSubRecords(ComparisonType.NOT_EQUAL, 61L).size());
        assertEquals(2, index.getSubRecords(ComparisonType.NOT_EQUAL, 66L).size());
        assertEquals(1, index.getSubRecords(ComparisonType.NOT_EQUAL, 555L).size());
        assertEquals(3, index.getRecords(new Comparable[]{66L, 555L, 34234L}).size());
        assertEquals(2, index.getRecords(new Comparable[]{555L, 34234L}).size());
        index.removeEntryIndex(record5.getIndexKey());
        assertEquals(2, index.getRecordValues().size());
        assertEquals(new Long(555L), index.getRecordValues().get(record50.getIndexKey()));
        assertEquals(null, index.getRecordValues().get(record5.getIndexKey()));
        records = index.getRecordMap(555L);
        assertNotNull(records);
        assertEquals(null, records.get(5L));
        assertEquals(record50, records.get(toData(50L)));
        assertEquals(1, index.getRecords(555L).size());
        assertEquals(2, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(2, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(1, index.getSubRecordsBetween(555L, 555L).size());
        assertEquals(0, index.getSubRecords(ComparisonType.LESSER, 66L).size());
        assertEquals(1, index.getSubRecords(ComparisonType.LESSER_EQUAL, 66L).size());
        assertEquals(1, index.getSubRecords(ComparisonType.LESSER_EQUAL, 67L).size());
        assertEquals(1, index.getSubRecords(ComparisonType.GREATER, 66L).size());
        assertEquals(2, index.getSubRecords(ComparisonType.GREATER_EQUAL, 66L).size());
        assertEquals(2, index.getSubRecords(ComparisonType.GREATER_EQUAL, 61L).size());
        index.removeEntryIndex(record50.getIndexKey());
        assertEquals(1, index.getRecordValues().size());
        assertEquals(null, index.getRecordValues().get(50L));
        records = index.getRecordMap(555L);
        assertNull(records);
        assertEquals(0, index.getRecords(555L).size());
        assertEquals(1, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(1, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(0, index.getSubRecordsBetween(555L, 555L).size());
        index.removeEntryIndex(record6.getIndexKey());
        assertEquals(0, index.getRecordValues().size());
        assertEquals(null, index.getRecordValues().get(6L));
        assertNull(index.getRecordMap(66L));
        assertEquals(0, index.getRecords(555L).size());
        assertEquals(0, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(0, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(0, index.getSubRecordsBetween(555L, 555L).size());
    }
}
