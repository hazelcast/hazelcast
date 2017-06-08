/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.impl.query.DefaultIndexProvider;
import com.hazelcast.map.impl.record.AbstractRecord;
import com.hazelcast.map.impl.record.DataRecordFactory;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.query.QueryConstants;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.getters.ReflectionHelper;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.instance.TestUtil.toData;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IndexTest {

    static final short FACTORY_ID = 1;

    final InternalSerializationService ss = new DefaultSerializationServiceBuilder()
            .addPortableFactory(FACTORY_ID, new TestPortableFactory()).build();

    private PartitioningStrategy partitionStrategy = new DefaultPartitioningStrategy();

    final DataRecordFactory recordFactory = new DataRecordFactory(new MapConfig(), ss, partitionStrategy);

    @Test
    public void testBasics() {
        testIt(true);
        testIt(false);
    }

    private QueryRecord newRecord(Object key, final Comparable attributeValue) {
        return new QueryRecord(toData(key), attributeValue);
    }

    @Test
    public void testRemoveEnumIndex() {
        Indexes is = new Indexes(ss, new DefaultIndexProvider(), Extractors.empty(), true);
        is.addOrGetIndex("favoriteCity", false);
        Data key = ss.toData(1);
        Data value = ss.toData(new SerializableWithEnum(SerializableWithEnum.City.ISTANBUL));
        is.saveEntryIndex(new QueryEntry(ss, key, value, Extractors.empty()), null);
        assertNotNull(is.getIndex("favoriteCity"));
        Record record = recordFactory.newRecord(value);
        ((AbstractRecord) record).setKey(key);
        is.removeEntryIndex(key, Records.getValueOrCachedValue(record, ss));
        assertEquals(0, is.getIndex("favoriteCity").getRecords(SerializableWithEnum.City.ISTANBUL).size());
    }

    @Test
    public void testUpdateEnumIndex() {
        Indexes is = new Indexes(ss, new DefaultIndexProvider(), Extractors.empty(), true);
        is.addOrGetIndex("favoriteCity", false);
        Data key = ss.toData(1);
        Data value = ss.toData(new SerializableWithEnum(SerializableWithEnum.City.ISTANBUL));
        is.saveEntryIndex(new QueryEntry(ss, key, value, Extractors.empty()), null);

        Data newValue = ss.toData(new SerializableWithEnum(SerializableWithEnum.City.KRAKOW));
        is.saveEntryIndex(new QueryEntry(ss, key, newValue, Extractors.empty()), value);

        assertEquals(0, is.getIndex("favoriteCity").getRecords(SerializableWithEnum.City.ISTANBUL).size());
        assertEquals(1, is.getIndex("favoriteCity").getRecords(SerializableWithEnum.City.KRAKOW).size());
    }

    @Test
    public void testIndex() throws QueryException {
        Indexes is = new Indexes(ss, new DefaultIndexProvider(), Extractors.empty(), true);
        Index dIndex = is.addOrGetIndex("d", false);
        Index boolIndex = is.addOrGetIndex("bool", false);
        Index strIndex = is.addOrGetIndex("str", false);
        for (int i = 0; i < 1000; i++) {
            Data key = ss.toData(i);
            Data value = ss.toData(new MainPortable(i % 2 == 0, -10.34d, "joe" + i));
            is.saveEntryIndex(new QueryEntry(ss, key, value, Extractors.empty()), null);
        }
        assertEquals(1000, dIndex.getRecords(-10.34d).size());
        assertEquals(1, strIndex.getRecords("joe23").size());
        assertEquals(500, boolIndex.getRecords(true).size());

        clearIndexes(dIndex, boolIndex, strIndex);

        for (int i = 0; i < 1000; i++) {
            Data key = ss.toData(i);
            Data value = ss.toData(new MainPortable(false, 11.34d, "joe"));
            is.saveEntryIndex(new QueryEntry(ss, key, value, Extractors.empty()), null);
        }

        assertEquals(0, dIndex.getRecords(-10.34d).size());
        assertEquals(0, strIndex.getRecords("joe23").size());
        assertEquals(1000, strIndex.getRecords("joe").size());
        assertEquals(1000, boolIndex.getRecords(false).size());
        assertEquals(0, boolIndex.getRecords(true).size());

        clearIndexes(dIndex, boolIndex, strIndex);

        for (int i = 0; i < 1000; i++) {
            Data key = ss.toData(i);
            Data value = ss.toData(new MainPortable(false, -1 * (i + 1), "joe" + i));
            is.saveEntryIndex(new QueryEntry(ss, key, value, Extractors.empty()), null);
        }
        assertEquals(0, dIndex.getSubRecordsBetween(1d, 1001d).size());
        assertEquals(1000, dIndex.getSubRecordsBetween(-1d, -1001d).size());
        clearIndexes(dIndex, boolIndex, strIndex);

        for (int i = 0; i < 1000; i++) {
            Data key = ss.toData(i);
            Data value = ss.toData(new MainPortable(false, 1 * (i + 1), "joe" + i));
            is.saveEntryIndex(new QueryEntry(ss, key, value, Extractors.empty()), null);
        }
        assertEquals(1000, dIndex.getSubRecordsBetween(1d, 1001d).size());
        assertEquals(0, dIndex.getSubRecordsBetween(-1d, -1001d).size());
        assertEquals(400, dIndex.getSubRecords(ComparisonType.GREATER, 600d).size());
        assertEquals(401, dIndex.getSubRecords(ComparisonType.GREATER_EQUAL, 600d).size());
        assertEquals(9, dIndex.getSubRecords(ComparisonType.LESSER, 10d).size());
        assertEquals(10, dIndex.getSubRecords(ComparisonType.LESSER_EQUAL, 10d).size());
        assertEquals(1, is.query(new AndPredicate(new EqualPredicate("d", 1d), new EqualPredicate("bool", "false"))).size());
        assertEquals(1, is.query(new AndPredicate(new EqualPredicate("d", 1), new EqualPredicate("bool", Boolean.FALSE))).size());
        assertEquals(1, is.query(new AndPredicate(new EqualPredicate("d", "1"), new EqualPredicate("bool", false))).size());
    }

    private void clearIndexes(Index... indexes) {
        for (Index index : indexes) {
            index.clear();
        }
    }

    @Test
    public void testIndexWithNull() throws QueryException {
        Indexes is = new Indexes(ss, new DefaultIndexProvider(), Extractors.empty(), true);
        Index strIndex = is.addOrGetIndex("str", true);

        Data value = ss.toData(new MainPortable(false, 1, null));
        Data key1 = ss.toData(0);
        is.saveEntryIndex(new QueryEntry(ss, key1, value, Extractors.empty()), null);

        value = ss.toData(new MainPortable(false, 2, null));
        Data key2 = ss.toData(1);
        is.saveEntryIndex(new QueryEntry(ss, key2, value, Extractors.empty()), null);


        for (int i = 2; i < 1000; i++) {
            Data key = ss.toData(i);
            value = ss.toData(new MainPortable(false, 1 * (i + 1), "joe" + i));
            is.saveEntryIndex(new QueryEntry(ss, key, value, Extractors.empty()), null);
        }

        assertEquals(2, strIndex.getRecords((Comparable) null).size());
        assertEquals(998, strIndex.getSubRecords(ComparisonType.NOT_EQUAL, null).size());
    }

    private class TestPortableFactory implements PortableFactory {

        public Portable create(int classId) {
            switch (classId) {
                case MainPortable.CLASS_ID:
                    return new MainPortable();
            }
            return null;
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }
    }

    private static class SerializableWithEnum implements DataSerializable {

        enum City {
            ISTANBUL,
            RIZE,
            KRAKOW,
            TRABZON
        }

        private City favoriteCity;

        private SerializableWithEnum() {
        }

        private SerializableWithEnum(City favoriteCity) {
            this.favoriteCity = favoriteCity;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(favoriteCity);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            favoriteCity = in.readObject();
        }
    }

    private static class MainPortable implements Portable {

        public static final int CLASS_ID = 1;

        byte b;
        boolean bool;
        char c;
        short s;
        int i;
        long l;
        float f;
        double d;
        String str;

        private MainPortable() {
        }

        private MainPortable(boolean bool, double d, String str) {
            this.bool = bool;
            this.d = d;
            this.str = str;
        }

        private MainPortable(byte b, boolean bool, char c, short s, int i, long l, float f, double d, String str) {
            this.b = b;
            this.bool = bool;
            this.c = c;
            this.s = s;
            this.i = i;
            this.l = l;
            this.f = f;
            this.d = d;
            this.str = str;
        }

        public int getClassId() {
            return CLASS_ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeByte("b", b);
            writer.writeBoolean("bool", bool);
            writer.writeChar("c", c);
            writer.writeShort("s", s);
            writer.writeInt("i", i);
            writer.writeLong("l", l);
            writer.writeFloat("f", f);
            writer.writeDouble("d", d);
            writer.writeUTF("str", str);
        }

        public void readPortable(PortableReader reader) throws IOException {
            b = reader.readByte("b");
            bool = reader.readBoolean("bool");
            c = reader.readChar("c");
            s = reader.readShort("s");
            i = reader.readInt("i");
            l = reader.readLong("l");
            f = reader.readFloat("f");
            d = reader.readDouble("d");
            str = reader.readUTF("str");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MainPortable that = (MainPortable) o;
            if (b != that.b) {
                return false;
            }
            if (bool != that.bool) {
                return false;
            }
            if (c != that.c) {
                return false;
            }
            if (Double.compare(that.d, d) != 0) {
                return false;
            }
            if (Float.compare(that.f, f) != 0) {
                return false;
            }
            if (i != that.i) {
                return false;
            }
            if (l != that.l) {
                return false;
            }
            if (s != that.s) {
                return false;
            }
            if (str != null ? !str.equals(that.str) : that.str != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = (int) b;
            result = 31 * result + (bool ? 1 : 0);
            result = 31 * result + (int) c;
            result = 31 * result + (int) s;
            result = 31 * result + i;
            result = 31 * result + (int) (l ^ (l >>> 32));
            result = 31 * result + (f != +0.0f ? Float.floatToIntBits(f) : 0);
            temp = d != +0.0d ? Double.doubleToLongBits(d) : 0L;
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            result = 31 * result + (str != null ? str.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "MainPortable{" +
                    "b=" + b +
                    ", bool=" + bool +
                    ", c=" + c +
                    ", s=" + s +
                    ", i=" + i +
                    ", l=" + l +
                    ", f=" + f +
                    ", d=" + d +
                    ", str='" + str + '\'' +
                    '}';
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }
    }

    class QueryRecord extends QueryableEntry {

        Data key;
        Comparable attributeValue;

        QueryRecord(Data key, Comparable attributeValue) {
            this.key = key;
            this.attributeValue = attributeValue;
        }

        public Comparable getAttributeValue(String attributeName) throws QueryException {
            return attributeValue;
        }

        public AttributeType getAttributeType(String attributeName) {
            return ReflectionHelper.getAttributeType(attributeValue.getClass());
        }

        @Override
        public Object getTargetObject(boolean key) {
            return key ? true : attributeValue;
        }

        public Data getKeyData() {
            return key;
        }

        public Data getValueData() {
            return null;
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
            return attributeValue;
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

        public Record toRecord() {
            Record<Data> record = recordFactory.newRecord(attributeValue);
            ((AbstractRecord) record).setKey(key);
            return record;
        }
    }

    private void testIt(boolean ordered) {
        IndexImpl index = new IndexImpl(QueryConstants.THIS_ATTRIBUTE_NAME.value(), ordered, ss, Extractors.empty());
        assertEquals(0, index.getRecords(0L).size());
        assertEquals(0, index.getSubRecordsBetween(0L, 1000L).size());
        QueryRecord record5 = newRecord(5L, 55L);
        index.saveEntryIndex(record5, null);
        assertEquals(Collections.<QueryableEntry>singleton(record5), index.getRecords(55L));

        QueryRecord record6 = newRecord(6L, 66L);
        index.saveEntryIndex(record6, null);

        assertEquals(Collections.<QueryableEntry>singleton(record6), index.getRecords(66L));

        QueryRecord newRecord5 = newRecord(5L, 555L);
        index.saveEntryIndex(newRecord5, record5.getValue());
        record5 = newRecord5;

        assertEquals(0, index.getRecords(55L).size());
        assertEquals(Collections.<QueryableEntry>singleton(record5), index.getRecords(555L));

        assertEquals(1, index.getRecords(555L).size());
        assertEquals(2, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(2, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(1, index.getSubRecordsBetween(555L, 555L).size());
        QueryRecord record50 = newRecord(50L, 555L);
        index.saveEntryIndex(record50, null);
        assertEquals(new HashSet<QueryableEntry>(asList(record5, record50)), index.getRecords(555L));

        Map<Data, QueryableEntry> records = getRecordMap(index, 555L);
        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(record5, records.get(record5.getKeyData()));
        assertEquals(record50, records.get(record50.getKeyData()));
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

        Record recordToRemove = record5.toRecord();
        index.removeEntryIndex(recordToRemove.getKey(), Records.getValueOrCachedValue(recordToRemove, ss));

        assertEquals(Collections.<QueryableEntry>singleton(record50), index.getRecords(555L));

        records = getRecordMap(index, 555L);
        assertNotNull(records);
        assertNull(records.get(5L));
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

        recordToRemove = record50.toRecord();
        index.removeEntryIndex(recordToRemove.getKey(), Records.getValueOrCachedValue(recordToRemove, ss));

        assertEquals(0, index.getRecords(555L).size());

        records = getRecordMap(index, 555L);
        assertNull(records);
        assertEquals(0, index.getRecords(555L).size());
        assertEquals(1, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(1, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(0, index.getSubRecordsBetween(555L, 555L).size());

        recordToRemove = record6.toRecord();
        index.removeEntryIndex(recordToRemove.getKey(), Records.getValueOrCachedValue(recordToRemove, ss));

        assertEquals(0, index.getRecords(66L).size());

        assertNull(getRecordMap(index, 66L));
        assertEquals(0, index.getRecords(555L).size());
        assertEquals(0, index.getSubRecordsBetween(55L, 555L).size());
        assertEquals(0, index.getSubRecordsBetween(66L, 555L).size());
        assertEquals(0, index.getSubRecordsBetween(555L, 555L).size());
    }

    private Map<Data, QueryableEntry> getRecordMap(IndexImpl index, Comparable indexValue) {
        Set<QueryableEntry> records = index.getRecords(indexValue);
        if (records.isEmpty()) {
            return null;
        }
        Map<Data, QueryableEntry> recordMap = new HashMap<Data, QueryableEntry>(records.size());
        for (QueryableEntry entry : records) {
            recordMap.put(entry.getKeyData(), entry);
        }
        return recordMap;
    }
}
