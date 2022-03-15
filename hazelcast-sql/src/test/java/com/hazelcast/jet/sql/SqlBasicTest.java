/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

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
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Test that covers basic column read operations through SQL.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("checkstyle:RedundantModifier")
public class SqlBasicTest extends SqlTestSupport {

    private static final int IDS_FACTORY_ID = 1;
    private static final int IDS_KEY_CLASS_ID = 2;
    private static final int IDS_VALUE_CLASS_ID = 3;
    static final int PORTABLE_FACTORY_ID = 1;
    private static final int PORTABLE_KEY_CLASS_ID = 2;
    static final int PORTABLE_VALUE_CLASS_ID = 3;
    private static final int PORTABLE_NESTED_CLASS_ID = 4;

    private static final String MAP_OBJECT = "map_object";
    private static final String MAP_BINARY = "map_binary";
    protected static final String MAP_TS = "map_tiered_store";

    protected static final int[] PAGE_SIZES = {256};
    protected static final int[] DATA_SET_SIZES = {4096};

    protected static HazelcastInstance member1;
    protected static HazelcastInstance member2;
    protected static HazelcastInstance client;

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
                    for (InMemoryFormat format : new InMemoryFormat[]{InMemoryFormat.OBJECT, InMemoryFormat.BINARY}) {
                        res.add(new Object[]{
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
        initializeWithClient(2, memberConfig(), clientConfig());

        member1 = instances()[0];
        member2 = instances()[1];

        client = client();
    }

    protected HazelcastInstance getTarget() {
        return member1;
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testSelect() {
        if (isPortable()) {
            createMapping(mapName(), PORTABLE_FACTORY_ID, PORTABLE_KEY_CLASS_ID, 0, PORTABLE_FACTORY_ID, PORTABLE_VALUE_CLASS_ID, 0);
        } else {
            createMapping(mapName(), keyClass(), valueClass());
        }

        // Get proper map
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName());

        // Populate map with values
        Map<Object, AbstractPojo> data = new HashMap<>();

        for (long i = 0; i < dataSetSize; i++) {
            data.put(key(i), value(i));
        }

        map.putAll(data);

        assertEquals(dataSetSize, map.size());

        // Execute query
        boolean portable = serializationMode == SerializationMode.PORTABLE;

        boolean multiPageClient;

        try (SqlResult res = query()) {
            multiPageClient = memberClientCursors() > 0;

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

                checkRowValue(SqlColumnType.BIGINT, key.getKey(), row, "key");
                checkRowValue(SqlColumnType.BOOLEAN, val.isBooleanVal(), row, "booleanVal");
                checkRowValue(SqlColumnType.TINYINT, val.getTinyIntVal(), row, "tinyIntVal");
                checkRowValue(SqlColumnType.SMALLINT, val.getSmallIntVal(), row, "smallIntVal");
                checkRowValue(SqlColumnType.INTEGER, val.getIntVal(), row, "intVal");
                checkRowValue(SqlColumnType.BIGINT, val.getBigIntVal(), row, "bigIntVal");
                checkRowValue(SqlColumnType.REAL, val.getRealVal(), row, "realVal");
                checkRowValue(SqlColumnType.DOUBLE, val.getDoubleVal(), row, "doubleVal");

                if (!portable) {
                    checkRowValue(SqlColumnType.DECIMAL, new BigDecimal(val.getDecimalBigIntegerVal()), row, "decimalBigIntegerVal");
                }
                checkRowValue(SqlColumnType.DECIMAL, val.getDecimalVal(), row, "decimalVal");

                checkRowValue(SqlColumnType.VARCHAR, Character.toString(val.getCharVal()), row, "charVal");
                checkRowValue(SqlColumnType.VARCHAR, val.getVarcharVal(), row, "varcharVal");

                checkRowValue(SqlColumnType.DATE, val.getDateVal(), row, "dateVal");
                checkRowValue(SqlColumnType.TIME, val.getTimeVal(), row, "timeVal");
                checkRowValue(SqlColumnType.TIMESTAMP, val.getTimestampVal(), row, "timestampVal");
                if (portable) {
                    checkRowValue(
                            SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                            val.getTsTzOffsetDateTimeVal(),
                            row,
                            "tsTzOffsetDateTimeVal"
                    );

                    checkRowValue(SqlColumnType.OBJECT, ((PortablePojo) val).getPortableVal(), row, "portableVal");
                    checkRowValue(SqlColumnType.VARCHAR, null, row, "nullVal");
                } else {
                    checkRowValue(
                            SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                            OffsetDateTime.ofInstant(val.getTsTzDateVal().toInstant(), ZoneId.systemDefault()),
                            row,
                            "tsTzDateVal"
                    );

                    checkRowValue(
                            SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                            val.getTsTzCalendarVal().toZonedDateTime().toOffsetDateTime(),
                            row,
                            "tsTzCalendarVal"
                    );

                    checkRowValue(
                            SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                            OffsetDateTime.ofInstant(val.getTsTzInstantVal(), ZoneId.systemDefault()),
                            row,
                            "tsTzInstantVal"
                    );

                    checkRowValue(
                            SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                            val.getTsTzOffsetDateTimeVal(),
                            row,
                            "tsTzOffsetDateTimeVal"
                    );

                    checkRowValue(
                            SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                            val.getTsTzZonedDateTimeVal().toOffsetDateTime(),
                            row,
                            "tsTzZonedDateTimeVal"
                    );

                    checkRowValue(SqlColumnType.OBJECT, val.getObjectVal(), row, "objectVal");
                    checkRowValue(SqlColumnType.OBJECT, null, row, "nullVal");
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

        if (multiPageClient) {
            // If this request spawns multiple pages, then:
            // 1) Ensure that results are cleared when the whole result set is fetched
            // 2) Ensure that results are cleared when the result set is closed in the middle.
            assertEquals(0, memberClientCursors());

            try (SqlResult res = query()) {
                assertEquals(1, memberClientCursors());

                res.close();

                assertEquals(0, memberClientCursors());
            }
        }
    }

    private int memberClientCursors() {
        return sqlInternalService(member1).getClientStateRegistry().getCursorCount()
                + sqlInternalService(member2).getClientStateRegistry().getCursorCount();
    }

    private void checkRowValue(SqlColumnType expectedType, Object expectedValue, SqlRow row, String columnName) {
        columnName = adjustFieldName(columnName);

        int columnIndex = row.getMetadata().findColumn(columnName);
        assertNotEquals(SqlRowMetadata.COLUMN_NOT_FOUND, columnIndex);

        assertEquals(expectedType, row.getMetadata().getColumn(columnIndex).getType());

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
            assertTrue(columnMetadata.isNullable());
        }

        assertThrows(IndexOutOfBoundsException.class, () -> rowMetadata.getColumn(-1));

        assertThrows(IndexOutOfBoundsException.class, () -> rowMetadata.getColumn(fields.size()));
    }

    private SqlResult query() {
        String sql = sql();

        if (cursorBufferSize == SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE) {
            return getTarget().getSql().execute(sql);
        } else {
            return getTarget().getSql().execute(new SqlStatement(sql).setCursorBufferSize(cursorBufferSize));
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
                    "decimalVal",
                    "charVal",
                    "varcharVal",
                    "dateVal",
                    "timeVal",
                    "timestampVal",
                    "tsTzOffsetDateTimeVal",
                    "portableVal",
                    "nullVal"
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
                    "objectVal",
                    "nullVal"
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
                    SqlColumnType.INTEGER,
                    SqlColumnType.BIGINT,
                    SqlColumnType.REAL,
                    SqlColumnType.DOUBLE,
                    SqlColumnType.DECIMAL,
                    SqlColumnType.VARCHAR,
                    SqlColumnType.VARCHAR,
                    SqlColumnType.DATE,
                    SqlColumnType.TIME,
                    SqlColumnType.TIMESTAMP,
                    SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                    SqlColumnType.OBJECT,
                    SqlColumnType.VARCHAR
            );
        } else {
            return Arrays.asList(
                    SqlColumnType.BIGINT,
                    SqlColumnType.BOOLEAN,
                    SqlColumnType.TINYINT,
                    SqlColumnType.SMALLINT,
                    SqlColumnType.INTEGER,
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
                    SqlColumnType.OBJECT,
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

    protected String mapName() {
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

    private Class<?> keyClass() {
        switch (serializationMode) {
            case SERIALIZABLE:
                return SerializablePojoKey.class;

            case DATA_SERIALIZABLE:
                return DataSerializablePojoKey.class;

            case IDENTIFIED_DATA_SERIALIZABLE:
                return IdentifiedDataSerializablePojoKey.class;

            default:
                return PortablePojoKey.class;
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

    private Class<?> valueClass() {
        switch (serializationMode) {
            case SERIALIZABLE:
                return SerializablePojo.class;

            case DATA_SERIALIZABLE:
                return DataSerializablePojo.class;

            case IDENTIFIED_DATA_SERIALIZABLE:
                return IdentifiedDataSerializablePojo.class;

            default:
                return PortablePojo.class;
        }
    }

    public static SerializationConfig serializationConfig() {
        SerializationConfig serializationConfig = new SerializationConfig();

        ClassDefinition nestedClassDefinition = new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, PORTABLE_NESTED_CLASS_ID, 0)
                .addIntField("val")
                .build();
        ClassDefinition valueClassDefinition = new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, PORTABLE_VALUE_CLASS_ID, 0)
                .addBooleanField(portableFieldName("booleanVal"))
                .addByteField(portableFieldName("tinyIntVal"))
                .addShortField(portableFieldName("smallIntVal"))
                .addIntField(portableFieldName("intVal"))
                .addLongField(portableFieldName("bigIntVal"))
                .addFloatField(portableFieldName("realVal"))
                .addDoubleField(portableFieldName("doubleVal"))
                .addDecimalField(portableFieldName("decimalVal"))
                .addCharField(portableFieldName("charVal"))
                .addStringField(portableFieldName("varcharVal"))
                .addDateField(portableFieldName("dateVal"))
                .addTimeField(portableFieldName("timeVal"))
                .addTimestampField(portableFieldName("timestampVal"))
                .addTimestampWithTimezoneField(portableFieldName("tsTzOffsetDateTimeVal"))
                .addPortableField(portableFieldName("portableVal"), nestedClassDefinition)
                .addStringField(portableFieldName("nullVal"))
                .build();
        ClassDefinition keyClassDefinition = new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, PORTABLE_KEY_CLASS_ID, 0)
                .addLongField(portableFieldName("key"))
                .build();

        serializationConfig.addClassDefinition(nestedClassDefinition);
        serializationConfig.addClassDefinition(valueClassDefinition);
        serializationConfig.addClassDefinition(keyClassDefinition);

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

        return serializationConfig;
    }

    static Config memberConfig() {
        MapConfig tsMapConfig = new MapConfig(MAP_TS).setInMemoryFormat(InMemoryFormat.NATIVE);
        tsMapConfig.getTieredStoreConfig().setEnabled(true);

        return smallInstanceConfig()
                .addMapConfig(new MapConfig(MAP_OBJECT).setInMemoryFormat(InMemoryFormat.OBJECT))
                .addMapConfig(new MapConfig(MAP_BINARY).setInMemoryFormat(InMemoryFormat.BINARY))
                .addMapConfig(tsMapConfig)
                .setSerializationConfig(serializationConfig());
    }

    protected static ClientConfig clientConfig() {
        return new ClientConfig().setSerializationConfig(serializationConfig());
    }

    private String adjustFieldName(String fieldName) {
        if (isPortable()) {
            fieldName = portableFieldName(fieldName);
        }

        return fieldName;
    }

    static String portableFieldName(String fieldName) {
        return fieldName + "_p";
    }

    abstract static class AbstractPojoKey implements Serializable {

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

    abstract static class AbstractPojo implements Serializable {

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

    static class SerializablePojo extends AbstractPojo implements Serializable {

        public SerializablePojo() {
            // no-op
        }

        public SerializablePojo(long val) {
            super(val);
        }
    }

    static class DataSerializablePojoKey extends AbstractPojoKey implements DataSerializable {
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

    static class DataSerializablePojo extends AbstractPojo implements DataSerializable {
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

    static class IdentifiedDataSerializablePojoKey extends DataSerializablePojoKey implements IdentifiedDataSerializable {
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

    static class IdentifiedDataSerializablePojo extends DataSerializablePojo implements IdentifiedDataSerializable {
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

    static class PortablePojoKey extends AbstractPojoKey implements Portable {
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

    static class PortablePojo extends AbstractPojo implements Portable {

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

    static class PortablePojoNested implements Portable {
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

    public enum SerializationMode {
        SERIALIZABLE,
        DATA_SERIALIZABLE,
        IDENTIFIED_DATA_SERIALIZABLE,
        PORTABLE
    }
}
