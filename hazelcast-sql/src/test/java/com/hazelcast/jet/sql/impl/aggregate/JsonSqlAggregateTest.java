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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import junitparams.JUnitParamsRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@RunWith(JUnitParamsRunner.class)
public class JsonSqlAggregateTest extends SqlJsonTestSupport {
    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_jsonArrayAgg_emptyResult() {
        String name = createTable();

        assertRowsAnyOrder("SELECT JSON_ARRAYAGG(name) FROM " + name + " WHERE 1=2", singletonList(new Row((Object) null)));
        assertRowsAnyOrder("SELECT JSON_ARRAYAGG(name NULL ON NULL) FROM " + name + " WHERE 1=2", singletonList(new Row((Object) null)));
        assertRowsAnyOrder("SELECT JSON_ARRAYAGG(name ABSENT ON NULL) FROM " + name + " WHERE 1=2", singletonList(new Row((Object) null)));
        assertRowsAnyOrder("SELECT JSON_ARRAYAGG(name) FROM " + name + " WHERE name IS NULL", singletonList(new Row((Object) null)));
    }

    @Test
    public void test_jsonArrayAgg_nulls() {
        String name = createTable();

        assertRowsAnyOrder("SELECT JSON_ARRAYAGG(name NULL ON NULL) FROM " + name + " WHERE name IS NULL",
                singletonList(new Row(json("[null,null]"))));
    }

    @Test
    public void test_jsonArrayAgg_unordered() {
        String name = createTable();
        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ABSENT ON NULL) FROM " + name + " WHERE name = 'Alice'",
                singletonList(
                        new Row(json("[\"Alice\",\"Alice\",\"Alice\"]"))
                )
        );

        assertRowsAnyOrder(
                "SELECT name, JSON_ARRAYAGG(distance) FROM " + name + " WHERE name = 'Bob' GROUP BY name",
                singletonList(new Row("Bob", json("[3]")))
        );
    }

    @Test
    public void test_jsonArrayAgg_orderedBySameColumn() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY name ABSENT ON NULL) FROM " + name,
                singletonList(
                        new Row(json("[\"Alice\",\"Alice\",\"Alice\",\"Bob\"]"))
                )
        );

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY name NULL ON NULL) FROM " + name,
                singletonList(
                        new Row(json("[null,null,\"Alice\",\"Alice\",\"Alice\",\"Bob\"]"))
                )
        );

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY name) FROM " + name,
                singletonList(
                        new Row(json("[\"Alice\",\"Alice\",\"Alice\",\"Bob\"]"))
                )
        );
    }

    @Test
    public void test_jsonArrayAgg_orderedByDifferentColumn() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY distance ABSENT ON NULL) FROM " + name,
                singletonList(
                        new Row(json("[\"Alice\",\"Bob\",\"Alice\",\"Alice\"]"))
                )
        );

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY distance NULL ON NULL) FROM " + name,
                singletonList(
                        new Row(json("[\"Alice\",\"Bob\",\"Alice\",null,\"Alice\",null]"))
                )
        );

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY distance) FROM " + name,
                singletonList(
                        new Row(json("[\"Alice\",\"Bob\",\"Alice\",\"Alice\"]"))
                )
        );
    }

    @Test
    public void test_jsonArrayAgg_withGroupBy() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT name, JSON_ARRAYAGG(distance ORDER BY distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", json("[1,4,7]")),
                        new Row("Bob", json("[3]")),
                        new Row(null, json("[6,8]"))

                )
        );

        assertRowsAnyOrder(
                "SELECT name, JSON_ARRAYAGG(distance ORDER BY distance DESC) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", json("[7,4,1]")),
                        new Row("Bob", json("[3]")),
                        new Row(null, json("[8,6]"))

                )
        );
    }

    @Test
    public void test_jsonArrayAgg_multiple() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT name, " +
                        "JSON_ARRAYAGG(distance ORDER BY distance DESC), " +
                        "JSON_ARRAYAGG(distance ORDER BY distance ASC) " +
                        "FROM " + name + " " +
                        "GROUP BY name",
                asList(
                        new Row("Alice", json("[7,4,1]"), json("[1,4,7]")),
                        new Row("Bob", json("[3]"), json("[3]")),
                        new Row(null, json("[8,6]"), json("[6,8]"))

                )
        );
    }

    @Test
    public void test_jsonObjectAgg() {
        String name = createTable2();

        assertRowsWithJsonValues(
                "SELECT name, JSON_OBJECTAGG(k VALUE v ABSENT ON NULL) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", json("{ \"department\" : \"dep1\", \"job\" : \"job1\", \"description\" : \"desc1\" }")),
                        new Row("Bob", json("{ \"department\" : \"dep2\", \"job\" : \"job2\" }")),
                        new Row(null, json("{ \"department\" : \"dep2\", \"job\" : \"job1\" }"))

                )
        );

        assertRowsWithJsonValues(
                "SELECT name, JSON_OBJECTAGG(k VALUE v NULL ON NULL) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", json("{ \"department\" : \"dep1\", \"job\" : \"job1\", \"description\" : \"desc1\" }")),
                        new Row("Bob", json("{ \"department\" : \"dep2\", \"job\" : \"job2\", \"description\" : null }")),
                        new Row(null, json("{ \"department\" : \"dep2\", \"job\" : \"job1\" }"))

                )
        );
    }

    @Test
    public void test_jsonObjectAgg_whenReturnsNull() {
        String name = createTable2();

        assertRowsAnyOrder(
                "SELECT name, JSON_OBJECTAGG(k VALUE v ABSENT ON NULL) FROM " + name + " WHERE name = 'Bob' AND k = 'description' GROUP BY name",
                asList(new Row("Bob", null))
        );
    }

    private static String createTable() {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("name", "distance"),
                asList(VARCHAR, INTEGER),
                asList(new String[]{"Alice", "1"},
                        new String[]{"Bob", "3"},
                        new String[]{"Alice", "4"},
                        new String[]{null, "6"},
                        new String[]{"Alice", "7"},
                        new String[]{null, "8"})
        );
        return name;
    }

    private static String createTable2() {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("name", "k", "v"),
                asList(VARCHAR, VARCHAR, VARCHAR),
                asList(new String[]{"Alice", "department", "dep1"},
                        new String[]{"Bob", "department", "dep2"},
                        new String[]{"Alice", "job", "job1"},
                        new String[]{null, "department", "dep2"},
                        new String[]{"Bob", "job", "job2"},
                        new String[]{null, "job", "job1"},
                        new String[]{null, null, "desc2"},
                        new String[]{"Bob", "description", null},
                        new String[]{"Alice", "description", "desc1"})
        );
        return name;
    }

    private void assertRowsWithJsonValues(String sql, Collection<Row> rows) {
        Map<Object, JsonValue> rowsInMap = new HashMap<>();
        for (Row row : rows) {
            Object[] rowObj = row.getValues();
            if (rowObj.length != 2) {
                throw new AssertionError("Row length must be 2");
            }
            if (!(rowObj[1] instanceof HazelcastJsonValue)) {
                throw new AssertionError("Second element in the row must be HazelcastJsonValue");
            }
            JsonValue value = Json.parse(((HazelcastJsonValue) rowObj[1]).getValue());
            rowsInMap.put(rowObj[0], value);
        }

        SqlResult result = sqlService.execute(sql);
        Map<String, JsonValue> actualRowsInMap = new HashMap<>();
        result.iterator().forEachRemaining(r -> {
            if (r.getMetadata().getColumnCount() != 2) {
                throw new AssertionError("The length of the result row is not 2");
            }
            JsonValue value = Json.parse(((HazelcastJsonValue) r.getObject(1)).getValue());
            actualRowsInMap.put(r.getObject(0), value);
        });

        if (rowsInMap.size() != actualRowsInMap.size()) {
            throw new AssertionError("Number of expected rows is different than actual");
        }
        for (Map.Entry<Object, JsonValue> kv : rowsInMap.entrySet()) {
            JsonValue value = actualRowsInMap.get(kv.getKey());
            JsonObject object = value.asObject();
            JsonObject object2 = kv.getValue().asObject();
            if (!(jsonObjectEqualsFirstLevelMixed(object, object2))) {
                throw new AssertionError("Object: " + object + " is not equal to Object: " + object2);
            }
        }
    }

    // A helper method to check whether two JSON objects are equal.
    // It checks whether first level of fields are same (although they can be
    // in mixed order).
    private boolean jsonObjectEqualsFirstLevelMixed(JsonObject obj, JsonObject obj2) {
        Map<String, JsonValue> fields = new HashMap<>();
        obj.iterator().forEachRemaining(m -> fields.put(m.getName(), m.getValue()));

        Map<String, JsonValue> fields2 = new HashMap<>();
        obj2.iterator().forEachRemaining(m -> fields2.put(m.getName(), m.getValue()));

        return fields.equals(fields2);
    }
}
