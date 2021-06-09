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

package com.hazelcast.sql;

import com.hazelcast.client.test.TestHazelcastFactory;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.config.IndexType.HASH;
import static com.hazelcast.config.IndexType.SORTED;
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
import static com.hazelcast.sql.SqlBasicTest.SerializationMode.IDENTIFIED_DATA_SERIALIZABLE;
import static com.hazelcast.sql.SqlBasicTest.SerializationMode.SERIALIZABLE;
import static com.hazelcast.sql.SqlBasicTest.serializationConfig;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    private static final int DATA_SET_MAX_POSITIVE = DATA_SET_SIZE / 2;

    private static final TestHazelcastFactory FACTORY = new TestHazelcastFactory();

    private static List<HazelcastInstance> members;

    @Parameter
    public SerializationMode serializationMode;

    @Parameter(1)
    public InMemoryFormat inMemoryFormat;

    @Parameter(2)
    public int membersCount;

    @Parameters(name = "serializationMode:{0}, inMemoryFormat:{1}, membersCount:{2}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (int membersCount : Collections.singletonList(1)) {
            for (SerializationMode serializationMode : Arrays.asList(SERIALIZABLE, IDENTIFIED_DATA_SERIALIZABLE)) {
                for (InMemoryFormat format : new InMemoryFormat[]{InMemoryFormat.OBJECT, InMemoryFormat.BINARY}) {
                    res.add(new Object[]{
                            serializationMode,
                            format,
                            membersCount
                    });
                }
            }
        }

        return res;
    }

    @Before
    public void before() {
        // Start members if needed
        if (members == null) {
            members = new ArrayList<>(membersCount);
            for (int i = 0; i < membersCount; ++i) {
                members.add(FACTORY.newHazelcastInstance(memberConfig()));
            }
        }

        // Get proper map
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName());

        // Populate map with values
        Map<Object, AbstractPojo> data = new HashMap<>();

        Random r = ThreadLocalRandom.current();
        int nextNullValue = Math.max(1, r.nextInt(5));
        int nextSameValues = Math.max(1, r.nextInt(5));
        long idx = Math.negateExact(DATA_SET_MAX_POSITIVE);
        int skipFirstPositiveEntries = 20;
        while (idx < DATA_SET_MAX_POSITIVE) {
            if (idx % nextNullValue == 0 && idx >= skipFirstPositiveEntries) {
                data.put(key(idx++), value());
                nextNullValue = Math.max(1, r.nextInt(5));
            } else if (idx % nextSameValues == 0 && idx >= skipFirstPositiveEntries) {
                int sameValuesCount = r.nextInt(5);
                long value = idx;
                while (sameValuesCount > 0 && idx < DATA_SET_MAX_POSITIVE) {
                    data.put(key(idx++), value(value));
                    sameValuesCount--;
                }
                nextSameValues = Math.max(1, r.nextInt(5));
            } else {
                data.put(key(idx), value(idx));
                idx++;
            }
        }
        map.putAll(data);

        // Populate stable map data
        Map<Object, AbstractPojo> stableData = new HashMap<>();

        idx = 0;
        while (idx < DATA_SET_SIZE) {
            stableData.put(key(idx), value(idx));
            idx++;
        }

        IMap<Object, AbstractPojo> stableMap = getTarget().getMap(stableMapName());
        stableMap.putAll(stableData);
    }

    @After
    public void after() {
        FACTORY.shutdownAll();
        members = null;
    }

    protected Config memberConfig() {
        Config config = new Config().setSerializationConfig(serializationConfig());

        config
                .addMapConfig(new MapConfig(MAP_OBJECT).setInMemoryFormat(InMemoryFormat.OBJECT))
                .addMapConfig(new MapConfig(MAP_BINARY).setInMemoryFormat(InMemoryFormat.BINARY));

        return config;
    }


    protected HazelcastInstance getTarget() {
        return members.get(0);
    }

    protected String stableMapName() {
        return inMemoryFormat == InMemoryFormat.OBJECT ? MAP_OBJECT + "_stable" : MAP_BINARY + "_stable";
    }

    @Test
    public void testSelectWithOrderByDesc() {
        checkSelectWithOrderBy(Collections.singletonList("intVal"),
                Collections.singletonList("intVal"), Collections.singletonList(true));
    }

    @Test
    public void testSelectWithOrderByAsc() {
        checkSelectWithOrderBy(Collections.singletonList("intVal"),
                Collections.singletonList("intVal"), Collections.singletonList(false));
    }

    @Test
    public void testSelectWithOrderByDefault() {
        checkSelectWithOrderBy(Collections.singletonList("intVal"),
                Collections.singletonList("intVal"), Collections.singletonList(null));
    }

    @Test
    public void testSelectWithOrderByDefaultAllTypes() {
        List<String> fields = Arrays.asList(
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
                "varcharVal");

        List<Boolean> orderDirections = new ArrayList<>(fields.size());
        fields.forEach(entry -> orderDirections.add(true));

        checkSelectWithOrderBy(fields, fields, orderDirections);
    }

    @Test
    public void testSelectWithOrderByDefaultTemporalTypes() {
        List<String> fields = Arrays.asList(
                "dateVal",
                "timeVal",
                "timestampVal",
                "tsTzOffsetDateTimeVal"
        );

        List<Boolean> orderDirections = new ArrayList<>(fields.size());
        fields.forEach(entry -> orderDirections.add(true));

        checkSelectWithOrderBy(fields, fields, orderDirections);
    }

    @Test
    public void testSelectWithOrderByDescDesc() {
        checkSelectWithOrderBy(Arrays.asList("intVal", "varcharVal"),
                Arrays.asList("intVal", "varcharVal"),
                Arrays.asList(true, true));
    }

    @Test
    public void testSelectWithOrderByAscDesc() {
        assertThrows(HazelcastSqlException.class,
                () -> checkSelectWithOrderBy(Arrays.asList("intVal", "varcharVal"),
                        Arrays.asList("intVal", "varcharVal"),
                        Arrays.asList(false, true)));
    }

    @Test
    public void testSelectWithOrderByDescDescDesc() {
        checkSelectWithOrderBy(Arrays.asList("intVal", "varcharVal", "bigIntVal"),
                Arrays.asList("intVal", "varcharVal", "bigIntVal"),
                Arrays.asList(true, true, true));
    }

    @Test
    public void testSelectWithOrderByDescDescAsc() {
        assertThrows(HazelcastSqlException.class,
                () -> checkSelectWithOrderBy(Arrays.asList("intVal", "varcharVal", "bigIntVal"),
                        Arrays.asList("intVal", "varcharVal", "bigIntVal"),
                        Arrays.asList(true, true, false)));
    }

    @Test
    public void testSelectWithOrderByNoIndex() {
        try {
            checkSelectWithOrderBy(Collections.emptyList(),
                    Arrays.asList("intVal"),
                    Arrays.asList(true));
            fail("Order by without matching index should fail");
        } catch (HazelcastSqlException e) {
            assertEquals("Cannot execute ORDER BY clause, because its input is not sorted. Consider adding a SORTED index to the data source.",
                    e.getMessage());
        }

        try {
            checkSelectWithOrderBy(Collections.emptyList(),
                    Arrays.asList("intVal", "realVal"),
                    Arrays.asList(true, true));
            fail("Order by without matching index should fail");
        } catch (HazelcastSqlException e) {
            assertEquals("Cannot execute ORDER BY clause, because its input is not sorted. Consider adding a SORTED index to the data source.",
                    e.getMessage());
        }
    }

    @Test
    public void testSelectWithOrderByAndProject() {
        // SELECT intVal, intVal + bigIntVal FROM t ORDER BY intVal, bigIntVal
        String sql = sqlWithOrderBy(Arrays.asList("intVal",
                "intVal + bigIntVal"),
                Arrays.asList("intVal", "bigIntVal"), Arrays.asList(true, true));

        checkSelectWithOrderBy(Arrays.asList("intVal", "bigIntVal"),
                sql,
                Arrays.asList("intVal"),
                Arrays.asList(true));
    }

    @Test
    public void testSelectWithOrderByAndProject2() {
        //SELECT a, b FROM (SELECT intVal+bigIntVal a, intVal-bigIntVal b FROM p) ORDER BY a, b"
        String sql = String.format("SELECT a, b FROM (SELECT intVal+bigIntVal a, intVal-bigIntVal b FROM %s) ORDER BY a, b", mapName());
        try {
            checkSelectWithOrderBy(Arrays.asList("intVal", "bigIntVal"),
                    sql,
                    Collections.emptyList(),
                    Collections.emptyList());
            fail("Order by on top of project should fail");
        } catch (HazelcastSqlException e) {
            assertEquals("Cannot execute ORDER BY clause, because its input is not sorted. Consider adding a SORTED index to the data source.", e.getMessage());
        }

    }

    @Test
    public void testSelectWithOrderByAndWhere() {
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName());
        String intValField = "intVal";
        String realValField = "realVal";
        addIndex(Arrays.asList(intValField), SORTED);
        addIndex(Arrays.asList(realValField), SORTED);

        String sql = "SELECT " + intValField + ", " + realValField + " FROM " + mapName()
                + " WHERE " + intValField + " = 1 ORDER BY " + realValField;

        assertSqlResultOrdered(sql, Arrays.asList(realValField), Arrays.asList(false), 1);
    }

    @Test
    public void testSelectWithOrderByAndWhereNotIndexedField() {
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName());
        String intValField = "intVal";
        String realValField = "realVal";
        addIndex(Arrays.asList(realValField), SORTED);

        String sql = "SELECT " + intValField + ", " + realValField + " FROM " + mapName()
                + " WHERE " + intValField + " = 1 ORDER BY " + realValField;

        assertSqlResultOrdered(sql, Arrays.asList(realValField), Arrays.asList(false), 1);
    }

    @Test
    public void testSelectWithOrderByAndWhere2Conditions() {
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName());
        String intValField = "intVal";
        String realValField = "realVal";
        addIndex(Arrays.asList(intValField, realValField), SORTED);

        String sql = "SELECT " + intValField + ", " + realValField + " FROM " + mapName()
                + " WHERE " + intValField + " = 1 AND " + realValField + " = 1 ORDER BY " + intValField;

        assertSqlResultOrdered(sql, Arrays.asList(realValField), Arrays.asList(false), 1);
    }

    @Test
    public void testSelectWithOrderByAndWhere2ConditionsHashIndex() {
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName());
        String intValField = "intVal";
        String realValField = "realVal";
        addIndex(Arrays.asList(intValField, realValField), HASH);
        addIndex(Arrays.asList(intValField), SORTED);

        String sql = "SELECT " + intValField + ", " + realValField + " FROM " + mapName()
                + " WHERE " + intValField + " = 1 AND " + realValField + " = 1 ORDER BY " + intValField;

        assertSqlResultOrdered(sql, Arrays.asList(realValField), Arrays.asList(false), 1);
    }

    @Test
    public void testSelectWithOrderByAndFetchOffset() {
        String intValField = "intVal";
        addIndex(Arrays.asList(intValField), SORTED, stableMapName());

        String sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " ORDER BY " + intValField + " OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY";

        assertSqlResultOrdered(sql, Arrays.asList(intValField), Arrays.asList(false), 10, 5, 14);

        String sqlLimit = "SELECT " + intValField + " FROM " + stableMapName()
                + " ORDER BY " + intValField + " LIMIT 10 OFFSET 5 ROWS";

        assertSqlResultOrdered(sqlLimit, Arrays.asList(intValField), Arrays.asList(false), 10, 5, 14);
    }

    @Test
    public void testSelectWithOrderByAndFetchOffsetNoResult() {
        String intValField = "intVal";
        addIndex(Arrays.asList(intValField), SORTED, stableMapName());

        String sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " ORDER BY " + intValField + " OFFSET 4096 ROWS FETCH FIRST 10 ROWS ONLY";

        assertSqlResultOrdered(sql, Arrays.asList(intValField), Arrays.asList(false), 0, 0, 0);

        String sqlLimit = "SELECT " + intValField + " FROM " + stableMapName()
                + " ORDER BY " + intValField + " LIMIT 10 OFFSET 4096 ROWS";

        assertSqlResultOrdered(sqlLimit, Arrays.asList(intValField), Arrays.asList(false), 0, 0, 0);
    }

    @Test
    public void testSelectWithOrderByAndFetchOffsetTail() {
        String intValField = "intVal";
        addIndex(Arrays.asList(intValField), SORTED, stableMapName());

        String sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " ORDER BY " + intValField + " OFFSET 4090 ROWS FETCH FIRST 10 ROWS ONLY";

        String sqlLimit = "SELECT " + intValField + " FROM " + stableMapName()
                + " ORDER BY " + intValField + " LIMIT 10 OFFSET 4090 ROWS";

        assertSqlResultOrdered(sql, Arrays.asList(intValField), Arrays.asList(false), 6, 4090, 4095);
        assertSqlResultOrdered(sqlLimit, Arrays.asList(intValField), Arrays.asList(false), 6, 4090, 4095);
    }

    @Test
    public void testSelectFetchOffsetOnly() {
        String intValField = "intVal";
        addIndex(Arrays.asList(intValField), SORTED, stableMapName());

        String sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " OFFSET 4090 ROWS FETCH FIRST 10 ROWS ONLY";

        assertSqlResultCount(sql, 6);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT 10 OFFSET 4090 ROWS";

        assertSqlResultCount(sql, 6);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY";

        assertSqlResultCount(sql, 10);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT 10 OFFSET 10 ROWS";

        assertSqlResultCount(sql, 10);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " OFFSET 10 ROWS";

        assertSqlResultCount(sql, 4086);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " OFFSET 4096 ROWS";

        assertSqlResultCount(sql, 0);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " FETCH FIRST 0 ROWS ONLY";

        assertSqlResultCount(sql, 0);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT 0";

        assertSqlResultCount(sql, 0);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " FETCH FIRST 100 ROWS ONLY";

        assertSqlResultCount(sql, 100);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT 100";

        assertSqlResultCount(sql, 100);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " FETCH FIRST 2.9 ROWS ONLY";

        assertSqlResultCount(sql, 2);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT 2.9";

        assertSqlResultCount(sql, 2);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " FETCH FIRST 1.2E2 ROWS ONLY";

        assertSqlResultCount(sql, 120);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT 1.2E2";

        assertSqlResultCount(sql, 120);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " FETCH FIRST 1.2E-2 ROWS ONLY";

        assertSqlResultCount(sql, 0);

        sql = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT 1.2E-2";

        assertSqlResultCount(sql, 0);
    }

    @Test
    public void testSelectFetchOffsetInvalid() {
        String intValField = "intVal";
        addIndex(Arrays.asList(intValField), SORTED, stableMapName());

        String sql1 = "SELECT " + intValField + " FROM " + stableMapName()
                + " OFFSET -5 ROWS FETCH FIRST 10 ROWS ONLY";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sql1, 0));

        String sqlLimit1 = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT 10 OFFSET -5 ROWS";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sqlLimit1, 0));

        String sql2 = "SELECT " + intValField + " FROM " + stableMapName()
                + " OFFSET 5 ROWS FETCH FIRST -10 ROWS ONLY";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sql2, 0));

        String sqlLimit2 = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT -10 OFFSET 5 ROWS";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sqlLimit2, 0));

        String sql3 = "SELECT " + intValField + " FROM " + stableMapName()
                + " OFFSET \"\" ROWS";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sql3, 0));

        String sql4 = "SELECT " + intValField + " FROM " + stableMapName()
                + " OFFSET intVal ROWS";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sql4, 0));

        String sql5 = "SELECT " + intValField + " FROM " + stableMapName()
                + " FETCH FIRST \"\" ROWS ONLY";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sql5, 0));

        String sqlLimit5 = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT \"\"";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sqlLimit5, 0));

        String sql6 = "SELECT " + intValField + " FROM " + stableMapName()
                + " FETCH FIRST null ROWS ONLY";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sql6, 0));

        String sqlLimit6 = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT null";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sqlLimit6, 0));

        String sql7 = "SELECT " + intValField + " FROM " + stableMapName()
                + " FETCH FIRST \"abc\" ROWS ONLY";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sql7, 0));

        String sqlLimit7 = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT \"abc\"";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sqlLimit7, 0));

        String sql8 = "SELECT " + intValField + " FROM " + stableMapName()
                + " FETCH FIRST 1 + ? ROWS ONLY";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sql8, 0));

        String sqlLimit8 = "SELECT " + intValField + " FROM " + stableMapName()
                + " LIMIT 1 + ?";

        assertThrows(HazelcastSqlException.class, () -> assertSqlResultCount(sqlLimit8, 0));
    }

    @Test
    public void testNestedFetchOffsetNotSupported() {
        String sql = "SELECT intVal FROM ( SELECT intVal FROM " + stableMapName()
                + " FETCH FIRST 5 ROWS ONLY)";

        assertThatThrownBy(() -> query(sql))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("FETCH/OFFSET is only supported for the top-level SELECT");

        String sqlLimit = "SELECT intVal FROM ( SELECT intVal FROM " + stableMapName() + " LIMIT 1)";

        assertThatThrownBy(() -> query(sqlLimit))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("FETCH/OFFSET is only supported for the top-level SELECT");
    }

    private void addIndex(List<String> fieldNames, IndexType type) {
        addIndex(fieldNames, type, mapName());
    }

    private void addIndex(List<String> fieldNames, IndexType type, String mapName) {
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName);

        IndexConfig indexConfig = new IndexConfig()
                .setName("Index_" + randomName())
                .setType(type);

        for (String fieldName : fieldNames) {
            indexConfig.addAttribute(fieldName);
        }

        map.addIndex(indexConfig);
    }

    protected void checkSelectWithOrderBy(List<String> indexAttrs, List<String> orderFields, List<Boolean> orderDirections) {
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName());

        IndexConfig indexConfig = new IndexConfig()
                .setName("Index_" + randomName())
                .setType(SORTED);

        for (String indexAttr : indexAttrs) {
            indexConfig.addAttribute(indexAttr);
        }
        if (indexAttrs.size() > 0) {
            map.addIndex(indexConfig);
        }

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
        assertSqlResultOrdered(sql, orderFields, orderDirections, map.size());
    }

    private void assertSqlResultOrdered(String sql, List<String> orderFields, List<Boolean> orderDirections,
                                        int expectedCount) {
        assertSqlResultOrdered(sql, orderFields, orderDirections, expectedCount, null, null);
    }

    private void assertSqlResultOrdered(String sql, List<String> orderFields, List<Boolean> orderDirections,
                                        int expectedCount,
                                        Integer low, Integer high) {
        try (SqlResult res = query(sql)) {

            SqlRowMetadata rowMetadata = res.getRowMetadata();

            Iterator<SqlRow> rowIterator = res.iterator();

            SqlRow prevRow = null;
            SqlRow lowRow = null;
            SqlRow highRow = null;
            int count = 0;
            while (rowIterator.hasNext()) {
                SqlRow row = rowIterator.next();
                assertOrdered(prevRow, row, orderFields, orderDirections, rowMetadata);

                prevRow = row;
                count++;
                if (count == 1) {
                    lowRow = row;
                }

                if (!rowIterator.hasNext()) {
                    highRow = row;
                }
            }
            assertEquals(expectedCount, count);
            if (lowRow != null && low != null) {
                String fieldName = orderFields.get(0);
                Object fieldValue = lowRow.getObject(rowMetadata.findColumn(fieldName));
                assertEquals(low, fieldValue);
            }

            if (highRow != null && high != null) {
                String fieldName = orderFields.get(0);
                Object fieldValue = highRow.getObject(rowMetadata.findColumn(fieldName));
                assertEquals(high, fieldValue);
            }

            assertThrows(NoSuchElementException.class, rowIterator::next);

            assertThrows(IllegalStateException.class, res::iterator);
        }
    }

    private void assertSqlResultCount(String sql, int expectedCount) {
        try (SqlResult res = query(sql)) {

            SqlRowMetadata rowMetadata = res.getRowMetadata();

            Iterator<SqlRow> rowIterator = res.iterator();

            int count = 0;
            while (rowIterator.hasNext()) {
                rowIterator.next();
                count++;

            }
            assertEquals(expectedCount, count);

            assertThrows(NoSuchElementException.class, rowIterator::next);

            assertThrows(IllegalStateException.class, res::iterator);
        }
    }

    private void checkSelectWithOrderBy(List<String> indexAttrs, String
            sql, List<String> checkOrderFields, List<Boolean> orderDirections
    ) {
        IMap<Object, AbstractPojo> map = getTarget().getMap(mapName());

        IndexConfig indexConfig = new IndexConfig()
                .setName("Index_" + randomName())
                .setType(SORTED);

        for (String indexAttr : indexAttrs) {
            indexConfig.addAttribute(indexAttr);
        }
        map.addIndex(indexConfig);

        assertEquals(DATA_SET_SIZE, map.size());

        assertSqlResultOrdered(sql, checkOrderFields, orderDirections, map.size());
    }

    private void assertOrdered(SqlRow prevRow, SqlRow row,
            List<String> orderFields, List<Boolean> orderDirections, SqlRowMetadata rowMetadata
    ) {
        if (prevRow == null) {
            return;
        }

        for (int i = 0; i < orderFields.size(); ++i) {
            String fieldName = orderFields.get(i);
            Boolean descending = orderDirections.get(i);
            Object prevFieldValue = prevRow.getObject(rowMetadata.findColumn(fieldName));
            Object fieldValue = row.getObject(rowMetadata.findColumn(fieldName));

            int cmp = 0;
            if (fieldValue == null) {
                // We use the default ordering for the null values, that is
                // null value is LESS than any other non-null value
                cmp = prevFieldValue == null ? 0 : 1;
            } else if (prevFieldValue == null) {
                cmp = fieldValue == null ? 0 : -1;
            } else {
                if (fieldValue instanceof Integer) {
                    cmp = ((Integer) prevFieldValue).compareTo((Integer) fieldValue);
                } else if (fieldValue instanceof Long) {
                    cmp = ((Long) prevFieldValue).compareTo((Long) fieldValue);
                } else if (fieldValue instanceof Float) {
                    cmp = ((Float) prevFieldValue).compareTo((Float) fieldValue);
                } else if (fieldValue instanceof Double) {
                    cmp = ((Double) prevFieldValue).compareTo((Double) fieldValue);
                } else if (fieldValue instanceof String) {
                    cmp = ((String) prevFieldValue).compareTo((String) fieldValue);
                } else if (fieldValue instanceof Boolean) {
                    cmp = ((Boolean) prevFieldValue).compareTo((Boolean) fieldValue);
                } else if (fieldValue instanceof Byte) {
                    cmp = ((Byte) prevFieldValue).compareTo((Byte) fieldValue);
                } else if (fieldValue instanceof Short) {
                    cmp = ((Short) prevFieldValue).compareTo((Short) fieldValue);
                } else if (fieldValue instanceof BigDecimal) {
                    cmp = ((BigDecimal) prevFieldValue).compareTo((BigDecimal) fieldValue);
                } else if (fieldValue instanceof LocalTime) {
                    cmp = ((LocalTime) prevFieldValue).compareTo((LocalTime) fieldValue);
                } else if (fieldValue instanceof LocalDate) {
                    cmp = ((LocalDate) prevFieldValue).compareTo((LocalDate) fieldValue);
                } else if (fieldValue instanceof LocalDateTime) {
                    cmp = ((LocalDateTime) prevFieldValue).compareTo((LocalDateTime) fieldValue);
                } else if (fieldValue instanceof OffsetDateTime) {
                    cmp = ((OffsetDateTime) prevFieldValue).compareTo((OffsetDateTime) fieldValue);
                } else {
                    fail("Not supported field type " + fieldValue.getClass());
                }
            }

            if (cmp == 0) {
                // Proceed with the next field
                continue;
            } else if (cmp < 0) {
                if (descending != null && descending) {
                    fail("For field " + fieldName + " the values " + prevFieldValue + ", " + fieldValue + " are not ordered descending");
                }
                return;
            } else if (cmp > 0) {
                if (descending == null || !descending) {
                    fail("For field " + fieldName + " the values " + prevFieldValue + ", " + fieldValue + " are not ordered ascending");
                }
                return;
            }
        }
    }

    protected SqlResult query(String sql) {
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

    private String basicSql() {
        List<String> fields = fields();

        StringBuilder res = new StringBuilder("SELECT ");

        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);

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

    private AbstractPojo value() {
        return value(null);
    }

    private AbstractPojo value(Long i) {
        switch (serializationMode) {
            case SERIALIZABLE:
                return i == null ? new SerializablePojo() : new SerializablePojo(i);

            case DATA_SERIALIZABLE:
                return i == null ? new DataSerializablePojo() : new DataSerializablePojo(i);

            case IDENTIFIED_DATA_SERIALIZABLE:
                return i == null ? new IdentifiedDataSerializablePojo() : new IdentifiedDataSerializablePojo(i);

            default:
                return i == null ? new PortablePojo() : new PortablePojo(i);
        }
    }
}
