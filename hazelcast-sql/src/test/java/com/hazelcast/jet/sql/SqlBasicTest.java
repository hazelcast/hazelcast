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
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
    private static final int[] PAGE_SIZES = {256};
    private static final int[] DATA_SET_SIZES = {4096};
    private static final TestHazelcastFactory FACTORY = new TestHazelcastFactory();

    private static HazelcastInstance member1;
    private static HazelcastInstance member2;
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
        member1 = FACTORY.newHazelcastInstance(memberConfig());
        member2 = FACTORY.newHazelcastInstance(memberConfig());

        client = FACTORY.newHazelcastClient(clientConfig());
    }

    @AfterClass
    public static void afterClass() {
        FACTORY.shutdownAll();
    }

    @Before
    public void before() {
        member1.getMap(MAP_OBJECT).clear();
        member1.getMap(MAP_BINARY).clear();
    }

    protected HazelcastInstance getTarget() {
        return member1;
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testSelect() {
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

                Long key0 = row.getObject(
                        rowMetadata.findColumn(
                                adjustFieldName("key", serializationMode)
                        )
                );
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
        columnName = adjustFieldName(columnName, serializationMode);

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
            String adjustedField = adjustFieldName(field, serializationMode);
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
            String field = adjustFieldName(fields.get(i), serializationMode);

            if (i != 0) {
                res.append(", ");
            }

            res.append(field);
        }

        res.append(" FROM ").append(mapName());

        return res.toString();
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

    public static SerializationConfig serializationConfig() {
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

        return serializationConfig;
    }

    static Config memberConfig() {
        return smallInstanceConfig()
                .addMapConfig(new MapConfig(MAP_OBJECT).setInMemoryFormat(InMemoryFormat.OBJECT))
                .addMapConfig(new MapConfig(MAP_BINARY).setInMemoryFormat(InMemoryFormat.BINARY))
                .setSerializationConfig(serializationConfig());
    }

    static ClientConfig clientConfig() {
        return new ClientConfig().setSerializationConfig(serializationConfig());
    }
}
