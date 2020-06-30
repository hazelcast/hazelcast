/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test that covers basic column read operations through SQL.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("checkstyle:RedundantModifier")
public class SqlBasicTest extends SqlTestSupport {

    private static final int IDS_FACTORY_ID = 1;
    private static final int IDS_KEY_CLASS_ID = 2;
    private static final int IDS_VALUE_CLASS_ID = 3;
    private static final int PORTABLE_FACTORY_ID = 1;
    private static final int PORTABLE_KEY_CLASS_ID = 2;
    private static final int PORTABLE_VALUE_CLASS_ID = 3;
    private static final int PORTABLE_NESTED_CLASS_ID = 4;

    private static final String MAP_OBJECT = "map_object";
    private static final String MAP_BINARY = "map_binary";

    private static final int[] PAGE_SIZES = { 1, 16, 256, 4096 };
    private static final int[] DATA_SET_SIZES = { 1, 256, 4096 };
    private static final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory(2);

    private static HazelcastInstance instance;

    @Parameter
    public int cursorBufferSize;

    @Parameter(1)
    public int dataSetSize;

    @Parameter(2)
    public SerializationMode serializationMode;

    @Parameter(3)
    public InMemoryFormat inMemoryFormat;

    @Parameters(name = "cursorBufferSize:{0}, dataSetSize:{1}, serializationMode:{2}, inMemoryFormat:{3}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (int pageSize : PAGE_SIZES) {
            for (int dataSetSize : DATA_SET_SIZES) {
                for (SerializationMode serializationMode : SerializationMode.values()) {
                    for (InMemoryFormat format : new InMemoryFormat[] { InMemoryFormat.OBJECT, InMemoryFormat.BINARY }) {
                        res.add(new Object[] {
                            pageSize,
                            dataSetSize,
                            serializationMode,
                            format
                        });
                    }
                }
            }
        }

        return res;
    }

    @BeforeClass
    public static void beforeClass() {
        instance = FACTORY.newHazelcastInstance(config());

        FACTORY.newHazelcastInstance(config());
    }

    @AfterClass
    public static void afterClass() {
        FACTORY.shutdownAll();
    }

    @Before
    public void before() {
        instance.getMap(MAP_OBJECT).clear();
        instance.getMap(MAP_BINARY).clear();
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testSelect() {
        // Get proper map
        IMap<Object, AbstractPojo> map = instance.getMap(mapName());

        // Populate map with values
        Map<Object, AbstractPojo> data = new HashMap<>();

        for (long i = 0; i < dataSetSize; i++) {
            data.put(key(i), value(i));
        }

        map.putAll(data);

        assertEquals(dataSetSize, map.size());

        // Execute query
        boolean portable = serializationMode == SerializationMode.PORTABLE;

        try (SqlResult res = query()) {
            SqlRowMetadata rowMetadata = res.getRowMetadata();

            checkRowMetadata(rowMetadata);

            Set<Long> uniqueKeys = new HashSet<>();

            Iterator<SqlRow> rowIterator = res.iterator();

            while (rowIterator.hasNext()) {
                SqlRow row = rowIterator.next();

                assertEquals(rowMetadata, res.getRowMetadata());

                Long key0 = row.getObject(rowMetadata.findColumn(adjustFieldName("key")));
                assertNotNull(key0);

                AbstractPojoKey key = key(key0);
                AbstractPojo val = map.get(key);

                checkRowValue(key.getKey(), row, "key");
                checkRowValue(val.isBooleanVal(), row, "booleanVal");
                checkRowValue(val.getTinyIntVal(), row, "tinyIntVal");
                checkRowValue(val.getSmallIntVal(), row, "smallIntVal");
                checkRowValue(val.getIntVal(), row, "intVal");
                checkRowValue(val.getBigIntVal(), row, "bigIntVal");
                checkRowValue(val.getRealVal(), row, "realVal");
                checkRowValue(val.getDoubleVal(), row, "doubleVal");

                if (!portable) {
                    checkRowValue(new BigDecimal(val.getDecimalBigIntegerVal()), row, "decimalBigIntegerVal");
                    checkRowValue(val.getDecimalVal(), row, "decimalVal");
                }

                checkRowValue(Character.toString(val.getCharVal()), row, "charVal");
                checkRowValue(val.getVarcharVal(), row, "varcharVal");

                if (!portable) {
                    checkRowValue(val.getDateVal(), row, "dateVal");
                    checkRowValue(val.getTimeVal(), row, "timeVal");
                    checkRowValue(val.getTimestampVal(), row, "timestampVal");

                    checkRowValue(
                        OffsetDateTime.ofInstant(val.getTsTzDateVal().toInstant(), ZoneId.systemDefault()),
                        row,
                        "tsTzDateVal"
                    );

                    checkRowValue(
                        val.getTsTzCalendarVal().toZonedDateTime().toOffsetDateTime(),
                        row,
                        "tsTzCalendarVal"
                    );

                    checkRowValue(
                        OffsetDateTime.ofInstant(val.getTsTzInstantVal(), ZoneId.systemDefault()),
                        row,
                        "tsTzInstantVal"
                    );

                    checkRowValue(
                        val.getTsTzOffsetDateTimeVal(),
                        row,
                        "tsTzOffsetDateTimeVal"
                    );

                    checkRowValue(
                        val.getTsTzZonedDateTimeVal().toOffsetDateTime(),
                        row,
                        "tsTzZonedDateTimeVal"
                    );

                    checkRowValue(val.getObjectVal(), row, "objectVal");
                }

                if (portable) {
                    checkRowValue(((PortablePojo) val).getPortableVal(), row, "portableVal");
                }

                uniqueKeys.add(key0);

                assertThrows(IndexOutOfBoundsException.class, () -> row.getObject(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> row.getObject(row.getMetadata().getColumnCount()));
                assertThrows(NullPointerException.class, () -> row.getObject(null));
                assertThrows(IllegalArgumentException.class, () -> row.getObject("unknown_field"));
            }

            assertThrows(NoSuchElementException.class, rowIterator::next);

            assertThrows(IllegalStateException.class, res::iterator);

            assertEquals(dataSetSize, uniqueKeys.size());
        }
    }

    private void checkRowValue(Object expectedValue, SqlRow row, String columnName) {
        columnName = adjustFieldName(columnName);

        int columnIndex = row.getMetadata().findColumn(columnName);
        assertNotEquals(SqlRowMetadata.COLUMN_NOT_FOUND, columnIndex);
        Object valueByIndex = row.getObject(columnIndex);
        assertEquals(expectedValue, valueByIndex);

        Object valueByName = row.getObject(columnIndex);
        assertEquals(expectedValue, valueByName);
    }

    private void checkRowMetadata(SqlRowMetadata rowMetadata) {
        List<String> fields = fields();
        List<SqlColumnType> fieldTypes = fieldTypes();

        assertEquals(fields.size(), rowMetadata.getColumnCount());

        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            String adjustedField = adjustFieldName(field);
            SqlColumnType fieldType = fieldTypes.get(i);

            int fieldIndex = rowMetadata.findColumn(adjustedField);

            assertNotEquals(SqlRowMetadata.COLUMN_NOT_FOUND, fieldIndex);

            SqlColumnMetadata columnMetadata = rowMetadata.getColumn(fieldIndex);
            assertEquals(adjustedField, columnMetadata.getName());
            assertEquals(fieldType, columnMetadata.getType());
        }

        assertThrows(IndexOutOfBoundsException.class, () -> rowMetadata.getColumn(-1));

        assertThrows(IndexOutOfBoundsException.class, () -> rowMetadata.getColumn(fields.size()));
    }

    private SqlResult query() {
        String sql = sql();

        if (cursorBufferSize == SqlQuery.DEFAULT_CURSOR_BUFFER_SIZE) {
            return instance.getSql().query(sql);
        } else {
            return instance.getSql().query(new SqlQuery(sql).setCursorBufferSize(cursorBufferSize));
        }
    }

    private List<String> fields() {
        if (serializationMode == SerializationMode.PORTABLE) {
            return Arrays.asList(
                "key",
                "booleanVal",
                "tinyIntVal",
                "smallIntVal",
                "intVal",
                "bigIntVal",
                "realVal",
                "doubleVal",
                "charVal",
                "varcharVal",
                "portableVal"
            );
        } else {
            return Arrays.asList(
                "key",
                "booleanVal",
                "tinyIntVal",
                "smallIntVal",
                "intVal",
                "bigIntVal",
                "realVal",
                "doubleVal",
                "decimalBigIntegerVal",
                "decimalVal",
                "charVal",
                "varcharVal",
                "dateVal",
                "timeVal",
                "timestampVal",
                "tsTzDateVal",
                "tsTzCalendarVal",
                "tsTzInstantVal",
                "tsTzOffsetDateTimeVal",
                "tsTzZonedDateTimeVal",
                "objectVal"
            );
        }
    }

    private List<SqlColumnType> fieldTypes() {
        if (serializationMode == SerializationMode.PORTABLE) {
            return Arrays.asList(
                SqlColumnType.BIGINT,
                SqlColumnType.BOOLEAN,
                SqlColumnType.TINYINT,
                SqlColumnType.SMALLINT,
                SqlColumnType.INT,
                SqlColumnType.BIGINT,
                SqlColumnType.REAL,
                SqlColumnType.DOUBLE,
                SqlColumnType.VARCHAR,
                SqlColumnType.VARCHAR,
                SqlColumnType.OBJECT
            );
        } else {
            return Arrays.asList(
                SqlColumnType.BIGINT,
                SqlColumnType.BOOLEAN,
                SqlColumnType.TINYINT,
                SqlColumnType.SMALLINT,
                SqlColumnType.INT,
                SqlColumnType.BIGINT,
                SqlColumnType.REAL,
                SqlColumnType.DOUBLE,
                SqlColumnType.DECIMAL,
                SqlColumnType.DECIMAL,
                SqlColumnType.VARCHAR,
                SqlColumnType.VARCHAR,
                SqlColumnType.DATE,
                SqlColumnType.TIME,
                SqlColumnType.TIMESTAMP,
                SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                SqlColumnType.OBJECT
            );
        }


    }

    private String sql() {
        List<String> fields = fields();

        StringBuilder res = new StringBuilder("SELECT ");

        for (int i = 0; i < fields.size(); i++) {
            String field = adjustFieldName(fields.get(i));

            if (i != 0) {
                res.append(", ");
            }

            res.append(field);
        }

        res.append(" FROM ").append(mapName());

        return res.toString();
    }

    private boolean isPortable() {
        return serializationMode == SerializationMode.PORTABLE;
    }

    private String mapName() {
        return inMemoryFormat == InMemoryFormat.OBJECT ? MAP_OBJECT : MAP_BINARY;
    }

    private AbstractPojoKey key(long i) {
        switch (serializationMode) {
            case SERIALIZABLE:
                return new SerializablePojoKey(i);

            case DATA_SERIALIZABLE:
                return new DataSerializablePojoKey(i);

            case IDENTIFIED_DATA_SERIALIZABLE:
                return new IdentifiedDataSerializablePojoKey(i);

            default:
                return new PortablePojoKey(i);
        }
    }

    private AbstractPojo value(long i) {
        switch (serializationMode) {
            case SERIALIZABLE:
                return new SerializablePojo(i);

            case DATA_SERIALIZABLE:
                return new DataSerializablePojo(i);

            case IDENTIFIED_DATA_SERIALIZABLE:
                return new IdentifiedDataSerializablePojo(i);

            default:
                return new PortablePojo(i);
        }
    }

    private static Config config() {
        SerializationConfig serializationConfig = new SerializationConfig();

        serializationConfig.addPortableFactory(PORTABLE_FACTORY_ID, classId -> {
            if (classId == PORTABLE_KEY_CLASS_ID) {
                return new PortablePojoKey();
            } else if (classId == PORTABLE_VALUE_CLASS_ID) {
                return new PortablePojo();
            } else if (classId == PORTABLE_NESTED_CLASS_ID) {
                return new PortablePojoNested();
            }

            throw new IllegalArgumentException("Unsupported class ID: " + classId);
        });

        serializationConfig.addDataSerializableFactory(IDS_FACTORY_ID, classId -> {
            if (classId == IDS_KEY_CLASS_ID) {
                return new IdentifiedDataSerializablePojoKey();
            } else if (classId == IDS_VALUE_CLASS_ID) {
                return new IdentifiedDataSerializablePojo();
            }

            throw new IllegalArgumentException("Unsupported class ID: " + classId);
        });

        Config config = new Config().setSerializationConfig(serializationConfig);

        config
            .addMapConfig(new MapConfig(MAP_OBJECT).setInMemoryFormat(InMemoryFormat.OBJECT))
            .addMapConfig(new MapConfig(MAP_BINARY).setInMemoryFormat(InMemoryFormat.BINARY));

        return config;
    }

    private String adjustFieldName(String fieldName) {
        if (isPortable()) {
            fieldName = portableFieldName(fieldName);
        }

        return fieldName;
    }

    private static String portableFieldName(String fieldName) {
        return fieldName + "_p";
    }

    private abstract static class AbstractPojoKey implements Serializable {

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
    }

    private abstract static class AbstractPojo implements Serializable {

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
    }

    public static class SerializablePojoKey extends AbstractPojoKey implements Serializable {
        public SerializablePojoKey(long key) {
            super(key);
        }
    }

    private static class SerializablePojo extends AbstractPojo implements Serializable {
        public SerializablePojo(long val) {
            super(val);
        }
    }

    private static class DataSerializablePojoKey extends AbstractPojoKey implements DataSerializable {
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

    private static class DataSerializablePojo extends AbstractPojo implements DataSerializable {
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
            out.writeUTF(varcharVal);

            out.writeObject(dateVal);
            out.writeObject(timeVal);
            out.writeObject(timestampVal);

            out.writeObject(tsTzDateVal);
            out.writeObject(tsTzCalendarVal);
            out.writeObject(tsTzInstantVal);
            out.writeObject(tsTzOffsetDateTimeVal);
            out.writeObject(tsTzZonedDateTimeVal);

            out.writeObject(objectVal);
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
            varcharVal = in.readUTF();

            dateVal = in.readObject();
            timeVal = in.readObject();
            timestampVal = in.readObject();

            tsTzDateVal = in.readObject();
            tsTzCalendarVal = in.readObject();
            tsTzInstantVal = in.readObject();
            tsTzOffsetDateTimeVal = in.readObject();
            tsTzZonedDateTimeVal = in.readObject();

            objectVal = in.readObject();
        }
    }

    private static class IdentifiedDataSerializablePojoKey extends DataSerializablePojoKey implements IdentifiedDataSerializable {
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

    private static class IdentifiedDataSerializablePojo extends DataSerializablePojo implements IdentifiedDataSerializable {
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

    private static class PortablePojoKey extends AbstractPojoKey implements Portable {
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

    private static class PortablePojo extends AbstractPojo implements Portable {

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

            writer.writeChar(portableFieldName("charVal"), charVal);
            writer.writeUTF(portableFieldName("varcharVal"), varcharVal);

            writer.writePortable(portableFieldName("portableVal"), portableVal);
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

            charVal = reader.readChar(portableFieldName("charVal"));
            varcharVal = reader.readUTF(portableFieldName("varcharVal"));

            portableVal = reader.readPortable(portableFieldName("portableVal"));
        }
    }

    private static class PortablePojoNested implements Portable {
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

    private enum SerializationMode {
        SERIALIZABLE,
        DATA_SERIALIZABLE,
        IDENTIFIED_DATA_SERIALIZABLE,
        PORTABLE
    }
}
