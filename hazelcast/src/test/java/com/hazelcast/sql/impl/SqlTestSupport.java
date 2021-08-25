/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Helper test classes.
 */
public class SqlTestSupport extends HazelcastTestSupport {

    @ClassRule
    public static OverridePropertyRule enableJetRule = OverridePropertyRule.set("hz.jet.enabled", "true");

    public static final int IDS_FACTORY_ID = 1;
    public static final int IDS_KEY_CLASS_ID = 2;
    public static final int IDS_VALUE_CLASS_ID = 3;
    public static final int PORTABLE_FACTORY_ID = 1;
    public static final int PORTABLE_KEY_CLASS_ID = 2;
    public static final int PORTABLE_VALUE_CLASS_ID = 3;
    public static final int PORTABLE_NESTED_CLASS_ID = 4;

    public static final String MAP_OBJECT = "map_object";
    public static final String MAP_BINARY = "map_binary";

    /**
     * Check object equality with additional hash code check.
     *
     * @param first    First object.
     * @param second   Second object.
     * @param expected Expected result.
     */
    public static void checkEquals(Object first, Object second, boolean expected) {
        if (expected) {
            assertEquals(first, second);
            assertEquals(first.hashCode(), second.hashCode());
        } else {
            assertNotEquals(first, second);
        }
    }

    public static InternalSerializationService getSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

    public static InternalSerializationService getSerializationService(HazelcastInstance instance) {
        return (InternalSerializationService) nodeEngine(instance).getSerializationService();
    }

    public static MapContainer getMapContainer(IMap<?, ?> map) {
        return ((MapProxyImpl<?, ?>) map).getService().getMapServiceContext().getMapContainer(map.getName());
    }

    public static <T> T serialize(Object original) {
        InternalSerializationService ss = getSerializationService();

        return getSerializationService().toObject(ss.toData(original));
    }

    public static <T> T serializeAndCheck(Object original, int expectedClassId) {
        assertTrue(original instanceof IdentifiedDataSerializable);

        IdentifiedDataSerializable original0 = (IdentifiedDataSerializable) original;

        assertEquals(SqlDataSerializerHook.F_ID, original0.getFactoryId());
        assertEquals(expectedClassId, original0.getClassId());

        return serialize(original);
    }

    public static QueryPath valuePath(String path) {
        return path(path, false);
    }

    public static QueryPath path(String path, boolean key) {
        return new QueryPath(path, key);
    }

    public static NodeEngineImpl nodeEngine(HazelcastInstance instance) {
        return Accessors.getNodeEngineImpl(instance);
    }

    public static Row row(Object... values) {
        assertNotNull(values);
        assertTrue(values.length > 0);

        return new HeapRow(values);
    }

    public static SqlInternalService sqlInternalService(HazelcastInstance instance) {
        return nodeEngine(instance).getSqlService().getInternalService();
    }

    public static List<SqlRow> execute(HazelcastInstance member, String sql, Object... params) {
        SqlStatement query = new SqlStatement(sql);

        if (params != null) {
            query.setParameters(Arrays.asList(params));
        }

        return executeStatement(member, query);
    }

    public static List<SqlRow> executeStatement(HazelcastInstance member, SqlStatement query) {
        List<SqlRow> rows = new ArrayList<>();

        try (SqlResult result = member.getSql().execute(query)) {
            for (SqlRow row : result) {
                rows.add(row);
            }
        }

        return rows;
    }

    public static void clearPlanCache(HazelcastInstance member) {
        ((SqlServiceImpl) member.getSql()).getPlanCache().clear();
    }

    public static PartitionIdSet getLocalPartitions(HazelcastInstance member) {
        PartitionService partitionService = member.getPartitionService();

        PartitionIdSet res = new PartitionIdSet(partitionService.getPartitions().size());

        for (Partition partition : partitionService.getPartitions()) {
            if (partition.getOwner().localMember()) {
                res.add(partition.getPartitionId());
            }
        }

        return res;
    }

    public static <K> K getLocalKey(
            HazelcastInstance member,
            IntFunction<K> keyProducer
    ) {
        return getLocalKeys(member, 1, keyProducer).get(0);
    }

    public static <K> List<K> getLocalKeys(
            HazelcastInstance member,
            int count,
            IntFunction<K> keyProducer
    ) {
        return new ArrayList<>(getLocalEntries(member, count, keyProducer, keyProducer).keySet());
    }

    public static <K, V> Map.Entry<K, V> getLocalEntry(
            HazelcastInstance member,
            IntFunction<K> keyProducer,
            IntFunction<V> valueProducer
    ) {
        return getLocalEntries(member, 1, keyProducer, valueProducer).entrySet().iterator().next();
    }

    public static <K, V> Map<K, V> getLocalEntries(
            HazelcastInstance member,
            int count,
            IntFunction<K> keyProducer,
            IntFunction<V> valueProducer
    ) {
        if (count == 0) {
            return Collections.emptyMap();
        }

        PartitionService partitionService = member.getPartitionService();

        Map<K, V> res = new LinkedHashMap<>();

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            K key = keyProducer.apply(i);

            if (key == null) {
                continue;
            }

            Partition partition = partitionService.getPartition(key);

            if (!partition.getOwner().localMember()) {
                continue;
            }

            V value = valueProducer.apply(i);

            if (value == null) {
                continue;
            }

            res.put(key, value);

            if (res.size() == count) {
                break;
            }
        }

        if (res.size() < count) {
            throw new RuntimeException("Failed to get the necessary number of keys: " + res.size());
        }

        return res;
    }

    public static boolean isPortable(SerializationMode serializationMode) {
        return serializationMode == SerializationMode.PORTABLE;
    }

    public String adjustFieldName(String fieldName, SerializationMode serializationMode) {
        if (isPortable(serializationMode)) {
            fieldName = portableFieldName(fieldName);
        }

        return fieldName;
    }

    static String portableFieldName(String fieldName) {
        return fieldName + "_p";
    }

    public enum SerializationMode {
        SERIALIZABLE,
        DATA_SERIALIZABLE,
        IDENTIFIED_DATA_SERIALIZABLE,
        PORTABLE
    }

    public abstract static class AbstractPojoKey implements Serializable {

        protected long key;

        protected AbstractPojoKey() {
            // No-op.
        }

        protected AbstractPojoKey(long key) {
            this.key = key;
        }

        public long getKey() {
            return key;
        }

        @Override
        public String toString() {
            return "AbstractPojoKey{key=" + key + '}';
        }
    }

    public abstract static class AbstractPojo implements Serializable {

        protected boolean booleanVal;

        protected byte tinyIntVal;
        protected short smallIntVal;
        protected int intVal;
        protected long bigIntVal;
        protected float realVal;
        protected double doubleVal;

        protected BigInteger decimalBigIntegerVal;
        protected BigDecimal decimalVal;

        protected char charVal;
        protected String varcharVal;

        protected LocalTime timeVal;
        protected LocalDate dateVal;
        protected LocalDateTime timestampVal;

        protected Date tsTzDateVal;
        protected GregorianCalendar tsTzCalendarVal;
        protected Instant tsTzInstantVal;
        protected OffsetDateTime tsTzOffsetDateTimeVal;
        protected ZonedDateTime tsTzZonedDateTimeVal;

        protected List<Object> objectVal;

        protected Object nullVal;

        protected AbstractPojo() {
            // No-op.
        }

        protected AbstractPojo(long val) {
            booleanVal = val % 2 == 0;

            tinyIntVal = (byte) val;
            smallIntVal = (short) val;
            intVal = (int) val;
            bigIntVal = val;
            realVal = (float) val;
            doubleVal = (double) val;

            decimalBigIntegerVal = BigInteger.valueOf(val);
            decimalVal = BigDecimal.valueOf(val);

            charVal = 'c';
            varcharVal = Long.toString(val);

            timestampVal = LocalDateTime.now();
            dateVal = timestampVal.toLocalDate();
            timeVal = timestampVal.toLocalTime();

            tsTzDateVal = new Date();
            tsTzCalendarVal = (GregorianCalendar) GregorianCalendar.getInstance();
            tsTzInstantVal = Instant.now();
            tsTzOffsetDateTimeVal = OffsetDateTime.now();
            tsTzZonedDateTimeVal = ZonedDateTime.now();

            objectVal = new ArrayList<>(1);
            objectVal.add(val);
        }

        public boolean isBooleanVal() {
            return booleanVal;
        }

        public byte getTinyIntVal() {
            return tinyIntVal;
        }

        public short getSmallIntVal() {
            return smallIntVal;
        }

        public int getIntVal() {
            return intVal;
        }

        public long getBigIntVal() {
            return bigIntVal;
        }

        public float getRealVal() {
            return realVal;
        }

        public double getDoubleVal() {
            return doubleVal;
        }

        public BigInteger getDecimalBigIntegerVal() {
            return decimalBigIntegerVal;
        }

        public BigDecimal getDecimalVal() {
            return decimalVal;
        }

        public char getCharVal() {
            return charVal;
        }

        public String getVarcharVal() {
            return varcharVal;
        }

        public LocalTime getTimeVal() {
            return timeVal;
        }

        public LocalDate getDateVal() {
            return dateVal;
        }

        public LocalDateTime getTimestampVal() {
            return timestampVal;
        }

        public Date getTsTzDateVal() {
            return tsTzDateVal;
        }

        public GregorianCalendar getTsTzCalendarVal() {
            return tsTzCalendarVal;
        }

        public Instant getTsTzInstantVal() {
            return tsTzInstantVal;
        }

        public OffsetDateTime getTsTzOffsetDateTimeVal() {
            return tsTzOffsetDateTimeVal;
        }

        public ZonedDateTime getTsTzZonedDateTimeVal() {
            return tsTzZonedDateTimeVal;
        }

        public List<Object> getObjectVal() {
            return objectVal;
        }

        @SuppressWarnings("unused")
        public Object getNullVal() {
            return nullVal;
        }
    }

    public static class SerializablePojoKey extends AbstractPojoKey implements Serializable {
        public SerializablePojoKey(long key) {
            super(key);
        }
    }

    public static class SerializablePojo extends AbstractPojo implements Serializable {

        public SerializablePojo() {
            // no-op
        }

        public SerializablePojo(long val) {
            super(val);
        }
    }

    public static class DataSerializablePojoKey extends AbstractPojoKey implements DataSerializable {
        public DataSerializablePojoKey() {
            // No-op.
        }

        public DataSerializablePojoKey(long key) {
            super(key);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(key);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            key = in.readLong();
        }
    }

    public static class DataSerializablePojo extends AbstractPojo implements DataSerializable {
        public DataSerializablePojo() {
            // No-op.
        }

        public DataSerializablePojo(long val) {
            super(val);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeBoolean(booleanVal);

            out.writeByte(tinyIntVal);
            out.writeShort(smallIntVal);
            out.writeInt(intVal);
            out.writeLong(bigIntVal);
            out.writeFloat(realVal);
            out.writeDouble(doubleVal);

            out.writeObject(decimalBigIntegerVal);
            out.writeObject(decimalVal);

            out.writeChar(charVal);
            out.writeString(varcharVal);

            out.writeObject(dateVal);
            out.writeObject(timeVal);
            out.writeObject(timestampVal);

            out.writeObject(tsTzDateVal);
            out.writeObject(tsTzCalendarVal);
            out.writeObject(tsTzInstantVal);
            out.writeObject(tsTzOffsetDateTimeVal);
            out.writeObject(tsTzZonedDateTimeVal);

            out.writeObject(objectVal);
            out.writeObject(nullVal);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            booleanVal = in.readBoolean();

            tinyIntVal = in.readByte();
            smallIntVal = in.readShort();
            intVal = in.readInt();
            bigIntVal = in.readLong();
            realVal = in.readFloat();
            doubleVal = in.readDouble();

            decimalBigIntegerVal = in.readObject();
            decimalVal = in.readObject();

            charVal = in.readChar();
            varcharVal = in.readString();

            dateVal = in.readObject();
            timeVal = in.readObject();
            timestampVal = in.readObject();

            tsTzDateVal = in.readObject();
            tsTzCalendarVal = in.readObject();
            tsTzInstantVal = in.readObject();
            tsTzOffsetDateTimeVal = in.readObject();
            tsTzZonedDateTimeVal = in.readObject();

            objectVal = in.readObject();
            nullVal = in.readObject();
        }
    }

    public static class IdentifiedDataSerializablePojoKey extends DataSerializablePojoKey implements IdentifiedDataSerializable {
        public IdentifiedDataSerializablePojoKey() {
            // No-op.
        }

        public IdentifiedDataSerializablePojoKey(long key) {
            super(key);
        }

        @Override
        public int getFactoryId() {
            return IDS_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return IDS_KEY_CLASS_ID;
        }
    }

    public static class IdentifiedDataSerializablePojo extends DataSerializablePojo implements IdentifiedDataSerializable {
        public IdentifiedDataSerializablePojo() {
            // No-op.
        }

        public IdentifiedDataSerializablePojo(long val) {
            super(val);
        }

        @Override
        public int getFactoryId() {
            return IDS_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return IDS_VALUE_CLASS_ID;
        }
    }

    public static class PortablePojoKey extends AbstractPojoKey implements Portable {
        public PortablePojoKey() {
            // No-op.
        }

        public PortablePojoKey(long key) {
            super(key);
        }

        @Override
        public int getFactoryId() {
            return PORTABLE_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return PORTABLE_KEY_CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeLong(portableFieldName("key"), key);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            key = reader.readLong(portableFieldName("key"));
        }
    }

    public static class PortablePojo extends AbstractPojo implements Portable {

        private PortablePojoNested portableVal;

        public PortablePojo() {
            // No-op.
        }

        public PortablePojo(long val) {
            super(val);

            portableVal = new PortablePojoNested((int) val);
        }

        public PortablePojoNested getPortableVal() {
            return portableVal;
        }

        @Override
        public int getFactoryId() {
            return PORTABLE_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return PORTABLE_VALUE_CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeBoolean(portableFieldName("booleanVal"), booleanVal);

            writer.writeByte(portableFieldName("tinyIntVal"), tinyIntVal);
            writer.writeShort(portableFieldName("smallIntVal"), smallIntVal);
            writer.writeInt(portableFieldName("intVal"), intVal);
            writer.writeLong(portableFieldName("bigIntVal"), bigIntVal);
            writer.writeFloat(portableFieldName("realVal"), realVal);
            writer.writeDouble(portableFieldName("doubleVal"), doubleVal);

            writer.writeDecimal(portableFieldName("decimalVal"), decimalVal);

            writer.writeChar(portableFieldName("charVal"), charVal);
            writer.writeString(portableFieldName("varcharVal"), varcharVal);

            writer.writeDate(portableFieldName("dateVal"), dateVal);
            writer.writeTime(portableFieldName("timeVal"), timeVal);
            writer.writeTimestamp(portableFieldName("timestampVal"), timestampVal);
            writer.writeTimestampWithTimezone(portableFieldName("tsTzOffsetDateTimeVal"), tsTzOffsetDateTimeVal);

            writer.writePortable(portableFieldName("portableVal"), portableVal);
            writer.writeString(portableFieldName("nullVal"), null);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            booleanVal = reader.readBoolean(portableFieldName("booleanVal"));

            tinyIntVal = reader.readByte(portableFieldName("tinyIntVal"));
            smallIntVal = reader.readShort(portableFieldName("smallIntVal"));
            intVal = reader.readInt(portableFieldName("intVal"));
            bigIntVal = reader.readLong(portableFieldName("bigIntVal"));
            realVal = reader.readFloat(portableFieldName("realVal"));
            doubleVal = reader.readDouble(portableFieldName("doubleVal"));

            decimalVal = reader.readDecimal(portableFieldName("decimalVal"));

            charVal = reader.readChar(portableFieldName("charVal"));
            varcharVal = reader.readString(portableFieldName("varcharVal"));

            dateVal = reader.readDate(portableFieldName("dateVal"));
            timeVal = reader.readTime(portableFieldName("timeVal"));
            timestampVal = reader.readTimestamp(portableFieldName("timestampVal"));
            tsTzOffsetDateTimeVal = reader.readTimestampWithTimezone(portableFieldName("tsTzOffsetDateTimeVal"));

            portableVal = reader.readPortable(portableFieldName("portableVal"));
            nullVal = reader.readString(portableFieldName("nullVal"));
        }
    }

    public static class PortablePojoNested implements Portable {
        private int val;

        public PortablePojoNested() {
            // No-op.
        }

        public PortablePojoNested(int val) {
            this.val = val;
        }

        @Override
        public int getFactoryId() {
            return PORTABLE_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return PORTABLE_NESTED_CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("val", val);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            val = reader.readInt("val");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PortablePojoNested that = (PortablePojoNested) o;

            return val == that.val;
        }

        @Override
        public int hashCode() {
            return val;
        }
    }

}
