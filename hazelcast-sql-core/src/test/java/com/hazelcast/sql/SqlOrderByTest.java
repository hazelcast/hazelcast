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
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.hazelcast.sql.SqlBasicTest.AbstractPojo;
import static com.hazelcast.sql.SqlBasicTest.AbstractPojoKey;
import static com.hazelcast.sql.SqlBasicTest.DataSerializablePojo;
import static com.hazelcast.sql.SqlBasicTest.DataSerializablePojoKey;
import static com.hazelcast.sql.SqlBasicTest.IdentifiedDataSerializablePojo;
import static com.hazelcast.sql.SqlBasicTest.IdentifiedDataSerializablePojoKey;
import static com.hazelcast.sql.SqlBasicTest.PortablePojo;
import static com.hazelcast.sql.SqlBasicTest.PortablePojoKey;
import static com.hazelcast.sql.SqlBasicTest.SerializablePojo;
import static com.hazelcast.sql.SqlBasicTest.SerializablePojoKey;
import static com.hazelcast.sql.SqlBasicTest.SerializationMode;
import static com.hazelcast.sql.SqlBasicTest.portableFieldName;
import static com.hazelcast.sql.SqlBasicTest.serializationConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test that covers basic column read operations through SQL.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("checkstyle:RedundantModifier")
public class SqlOrderByTest extends SqlTestSupport {

    private static final String MAP_OBJECT = "map_object";
    private static final String MAP_BINARY = "map_binary";

    private static final int DATA_SET_SIZE = 4096;
    private static final SqlTestInstanceFactory FACTORY = SqlTestInstanceFactory.create();

    private static HazelcastInstance member1;
    private static HazelcastInstance member2;

    @Parameter
    public SerializationMode serializationMode;

    @Parameter(1)
    public InMemoryFormat inMemoryFormat;

    @Parameters(name = "serializationMode:{0}, inMemoryFormat:{1}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (SerializationMode serializationMode : SerializationMode.values()) {
            for (InMemoryFormat format : new InMemoryFormat[]{InMemoryFormat.OBJECT, InMemoryFormat.BINARY}) {
                res.add(new Object[]{
                    serializationMode,
                    format
                });
            }
        }

        return res;
    }

    @Before
    public void before() {
        // Start members if needed
        if (member1 == null) {
            member1 = FACTORY.newHazelcastInstance(memberConfig());
            member2 = FACTORY.newHazelcastInstance(memberConfig());
        }

        clearMap();
        // Get proper map
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName());

        // Populate map with values
        Map<Object, AbstractPojo> data = new HashMap<>();

        for (long i = 0; i < DATA_SET_SIZE; i++) {
            data.put(key(i), value(i));
        }

        map.putAll(data);
    }

    protected void clearMap() {
        member1.getMap(MAP_OBJECT).clear();
        member1.getMap(MAP_BINARY).clear();
    }

    @AfterClass
    public static void afterClass() {
        FACTORY.shutdownAll();
    }

    protected Config memberConfig() {
        Config config = new Config().setSerializationConfig(serializationConfig());

        config
            .addMapConfig(new MapConfig(MAP_OBJECT).setInMemoryFormat(InMemoryFormat.OBJECT))
            .addMapConfig(new MapConfig(MAP_BINARY).setInMemoryFormat(InMemoryFormat.BINARY));

        return config;
    }


    protected HazelcastInstance getTarget() {
        return member1;
    }

    @Test
    public void testSelectWithOrderByDesc() {
        checkSelectWithOrderBy(Collections.singletonList(adjustFieldName("intVal")),
            Collections.singletonList(adjustFieldName("intVal")), Collections.singletonList(true));
    }

    @Test
    public void testSelectWithOrderByAsc() {
        checkSelectWithOrderBy(Collections.singletonList(adjustFieldName("intVal")),
            Collections.singletonList(adjustFieldName("intVal")), Collections.singletonList(false));
    }

    @Test
    public void testSelectWithOrderByDefault() {
        checkSelectWithOrderBy(Collections.singletonList(adjustFieldName("intVal")),
            Collections.singletonList(adjustFieldName("intVal")), Collections.singletonList(null));
    }

    @Test
    public void testSelectWithOrderByDescDesc() {
        checkSelectWithOrderBy(Arrays.asList(adjustFieldName("intVal"), adjustFieldName("varcharVal")),
            Arrays.asList(adjustFieldName("intVal"), adjustFieldName("varcharVal")),
            Arrays.asList(true, true));
    }

    @Test
    public void testSelectWithOrderByAscDesc() {
        assertThrows(HazelcastSqlException.class,
            () -> checkSelectWithOrderBy(Arrays.asList(adjustFieldName("intVal"), adjustFieldName("varcharVal")),
                Arrays.asList(adjustFieldName("intVal"), adjustFieldName("varcharVal")),
                Arrays.asList(false, true)));
    }

    @Test
    public void testSelectWithOrderByDescDescDesc() {
        checkSelectWithOrderBy(Arrays.asList(adjustFieldName("intVal"), adjustFieldName("varcharVal"), adjustFieldName("bigIntVal")),
            Arrays.asList(adjustFieldName("intVal"), adjustFieldName("varcharVal"), adjustFieldName("bigIntVal")),
            Arrays.asList(true, true, true));
    }

    @Test
    public void testSelectWithOrderByDescDescAsc() {
        assertThrows(HazelcastSqlException.class,
            () -> checkSelectWithOrderBy(Arrays.asList(adjustFieldName("intVal"), adjustFieldName("varcharVal"), adjustFieldName("bigIntVal")),
                Arrays.asList(adjustFieldName("intVal"), adjustFieldName("varcharVal"), adjustFieldName("bigIntVal")),
                Arrays.asList(true, true, false)));
    }

    @Test
    public void testSelectWithOrderByAndProject() {
        // SELECT intVal, intVal + bigIntVal FROM t ORDER BY intVal, bigIntVal
        String sql = sqlWithOrderBy(Arrays.asList(adjustFieldName("intVal"),
            adjustFieldName("intVal") + " + " + adjustFieldName("bigIntVal")),
            Arrays.asList(adjustFieldName("intVal"), adjustFieldName("bigIntVal")), Arrays.asList(true, true));
        checkSelectWithOrderBy(Arrays.asList(adjustFieldName("intVal"), adjustFieldName("bigIntVal")),
            sql,
            Arrays.asList(adjustFieldName("intVal"), adjustFieldName("bigIntVal")),
            Arrays.asList(true, true));
    }

    @Test
    public void testSelectWithOrderByAndProject2() {
        //SELECT a, b FROM (SELECT intVal+bigIntVal a, intVal-bigIntVal b FROM p) ORDER BY a, b"
        String sql = "SELECT a, b FROM (SELECT intVal+bigIntVal a, intVal-bigIntVal b FROM p) ORDER BY a, b";
        assertThrows(HazelcastSqlException.class,
            () -> checkSelectWithOrderBy(Arrays.asList(adjustFieldName("intVal"), adjustFieldName("bigIntVal")),
                sql,
                Collections.emptyList(),
                Collections.emptyList()));
    }

    @Test
    public void testSelectWithOrderByAndWhere() {
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName());

        String intValField = adjustFieldName("intVal");
        String bigIntValField = adjustFieldName("bigIntVal");
        IndexConfig indexConfig1 = new IndexConfig().setName("Index_" + randomName())
            .setType(IndexType.SORTED).addAttribute(intValField);

        IndexConfig indexConfig2 = new IndexConfig().setName("Index_" + randomName())
            .setType(IndexType.SORTED).addAttribute(bigIntValField);

        map.addIndex(indexConfig1);
        map.addIndex(indexConfig2);

        String sql = "SELECT " + intValField + ", " + bigIntValField + " FROM " + mapName()
            + " WHERE " + intValField + " = 1 ORDER BY " + bigIntValField;

        try (SqlResult res = query(sql)) {

            SqlRowMetadata rowMetadata = res.getRowMetadata();

            Iterator<SqlRow> rowIterator = res.iterator();

            SqlRow prevRow = null;
            while (rowIterator.hasNext()) {
                SqlRow row = rowIterator.next();

                assertOrdered(prevRow, row, Collections.singletonList("bigIntVal"),
                    Collections.singletonList(false), rowMetadata);

                prevRow = row;
            }

            assertThrows(NoSuchElementException.class, rowIterator::next);

            assertThrows(IllegalStateException.class, res::iterator);
        }

    }


    public void checkSelectWithOrderBy(List<String> indexAttrs, List<String> orderFields, List<Boolean> orderDirections) {
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName());

        IndexConfig indexConfig = new IndexConfig().setName("Index_" + randomName())
            .setType(IndexType.SORTED);

        for (String indexAttr : indexAttrs) {
            indexConfig.addAttribute(indexAttr);
        }
        map.addIndex(indexConfig);

        assertEquals(DATA_SET_SIZE, map.size());

        StringBuilder orders = new StringBuilder();
        for (int i = 0; i < orderFields.size(); ++i) {
            String orderField = orderFields.get(i);
            Boolean descending = orderDirections.get(i);
            orders.append(orderField);
            if (descending != null) {
                orders.append(descending ? " DESC " : " ASC ");
            }
            if (i < orderFields.size() - 1) {
                orders.append(", ");
            }
        }
        String sql = sqlWithOrderBy(orders.toString());
        try (SqlResult res = query(sql)) {

            SqlRowMetadata rowMetadata = res.getRowMetadata();

            Iterator<SqlRow> rowIterator = res.iterator();

            SqlRow prevRow = null;
            while (rowIterator.hasNext()) {
                SqlRow row = rowIterator.next();

                assertOrdered(prevRow, row, orderFields, orderDirections, rowMetadata);

                prevRow = row;
            }

            assertThrows(NoSuchElementException.class, rowIterator::next);

            assertThrows(IllegalStateException.class, res::iterator);
        }
    }

    public void checkSelectWithOrderBy(List<String> indexAttrs, String sql, List<String> orderFields, List<Boolean> orderDirections) {
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName());

        IndexConfig indexConfig = new IndexConfig().setName("Index_" + randomName())
            .setType(IndexType.SORTED);

        for (String indexAttr : indexAttrs) {
            indexConfig.addAttribute(indexAttr);
        }
        map.addIndex(indexConfig);

        assertEquals(DATA_SET_SIZE, map.size());

        try (SqlResult res = query(sql)) {

            SqlRowMetadata rowMetadata = res.getRowMetadata();

            Iterator<SqlRow> rowIterator = res.iterator();

            SqlRow prevRow = null;
            while (rowIterator.hasNext()) {
                SqlRow row = rowIterator.next();

                assertOrdered(prevRow, row, orderFields, orderDirections, rowMetadata);

                prevRow = row;
            }

            assertThrows(NoSuchElementException.class, rowIterator::next);

            assertThrows(IllegalStateException.class, res::iterator);
        }
    }

    private void assertOrdered(SqlRow prevRow, SqlRow row, List<String> orderFields, List<Boolean> orderDirections, SqlRowMetadata rowMetadata) {
        if (prevRow == null) {
            return;
        }

        for (int i = 0; i < orderFields.size(); ++i) {
            String fieldName = orderFields.get(i);
            Boolean descending = orderDirections.get(i);
            Object prevFieldValue = prevRow.getObject(rowMetadata.findColumn(fieldName));
            Object fieldValue = row.getObject(rowMetadata.findColumn(fieldName));

            int cmp = 0;
            if (fieldValue instanceof Integer) {
                cmp = ((Integer) prevFieldValue).compareTo((Integer) fieldValue);
            } else if (fieldValue instanceof Long) {
                cmp = ((Long) prevFieldValue).compareTo((Long) fieldValue);
            } else if (fieldValue instanceof String) {
                cmp = ((String) prevFieldValue).compareTo((String) fieldValue);
            } else {
                fail("Not supported field type");
            }

            if (cmp == 0) {
                // Proceed with the next field
                continue;
            } else if (cmp < 0) {
                if (descending != null && descending) {
                    fail("For field " + fieldName + " the values " + prevFieldValue + ", " + fieldValue + " are not ordered ascending");
                }
                return;
            } else if (cmp > 0) {
                if (descending == null || !descending) {
                    fail("For field " + fieldName + " the values " + prevFieldValue + ", " + fieldValue + " are not ordered descending");
                }
                return;
            }
        }
    }

    private SqlResult query(String sql) {
        return getTarget().getSql().execute(sql);
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

    private String basicSql() {
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

    private String sqlWithOrderBy(List<String> projects, List<String> orderFields, List<Boolean> orderDirections) {
        StringBuilder res = new StringBuilder("SELECT ");

        for (int i = 0; i < projects.size(); i++) {
            String field = projects.get(i);

            if (i != 0) {
                res.append(", ");
            }

            res.append(field);
        }

        res.append(" FROM ").append(mapName());

        res.append(" ORDER BY ");

        for (int i = 0; i < orderFields.size(); ++i) {
            String orderField = orderFields.get(i);
            Boolean descending = orderDirections.get(i);

            if (i != 0) {
                res.append(", ");
            }

            res.append(orderField);
            if (descending != null) {
                res.append(descending ? " DESC " : " ASC ");
            }
        }

        return res.toString();
    }


    private String sqlWithOrderBy(String orderCondition) {
        StringBuilder res = new StringBuilder(basicSql());

        res.append(" ORDER BY ").append(orderCondition);
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


    private String adjustFieldName(String fieldName) {
        if (isPortable()) {
            fieldName = portableFieldName(fieldName);
        }

        return fieldName;
    }

}
