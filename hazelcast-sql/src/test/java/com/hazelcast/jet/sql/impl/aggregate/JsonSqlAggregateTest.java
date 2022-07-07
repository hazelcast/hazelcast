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

import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlService;
import junitparams.JUnitParamsRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

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
        assertRowsAnyOrder("SELECT JSON_ARRAYAGG(name ABSENT ON NULL) from " + name + " WHERE 1=2", singletonList(new Row((Object) null)));
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
}
