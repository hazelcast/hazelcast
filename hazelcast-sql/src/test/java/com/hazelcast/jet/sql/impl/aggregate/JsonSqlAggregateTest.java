/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(Enclosed.class)
public class JsonSqlAggregateTest {

    @RunWith(HazelcastSerialClassRunner.class)
    @Category({QuickTest.class, ParallelJVMTest.class})
    public static class JsonArrayAggregationTest extends SqlJsonTestSupport {
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

        private String createTable() {
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

    @RunWith(HazelcastSerialClassRunner.class)
    @Category({QuickTest.class, ParallelJVMTest.class})
    public static class JsonObjectAggregationTest extends SqlJsonTestSupport {
        private static SqlService sqlService;

        @BeforeClass
        public static void setUpClass() {
            initialize(2, null);
            sqlService = instance().getSql();
        }

        @Test
        public void when_jsonObjectAgg_jsonInputClause_then_fail() {
            assertThatThrownBy(() -> sqlService.execute("SELECT JSON_OBJECTAGG('k' VALUE 'v' FORMAT JSON)"))
                    .hasMessage("From line 1, column 33 to line 1, column 47: JSON VALUE EXPRESSION not supported");
        }

        @Test
        public void when_jsonObjectAgg_keyUniquenessConstraint_then_fail() {
            assertThatThrownBy(() -> sqlService.execute("SELECT JSON_OBJECTAGG('k' VALUE 'v' WITH UNIQUE KEYS)"))
                    .hasMessageStartingWith("Encountered \"WITH\" at line 1, column 37");
        }

        @Test
        public void when_jsonObjectAgg_outputClause_then_fail() {
            assertThatThrownBy(() -> sqlService.execute("SELECT JSON_OBJECTAGG('k' VALUE 'v' RETURNING VARCHAR)"))
                    .hasMessageStartingWith("Encountered \"RETURNING\" at line 1, column 37.");
        }

        @Test
        public void test_literal() {
            assertJsonRowsAnyOrder("SELECT JSON_OBJECTAGG('k' VALUE 'v')", singletonList(new Row(json("{\"k\":\"v\"}"))));
        }

        @Test
        public void test_alternateSyntax() {
            assertJsonRowsAnyOrder("SELECT JSON_OBJECTAGG(key 'k' value 'v')", singletonList(new Row(json("{\"k\":\"v\"}"))));
            assertJsonRowsAnyOrder("SELECT JSON_OBJECTAGG('k':'v')", singletonList(new Row(json("{\"k\":\"v\"}"))));
        }

        @Test
        public void test_nullKey() {
            // null literal key
            assertThatThrownBy(() -> sqlService.execute("SELECT JSON_OBJECTAGG(NULL VALUE 'v')").iterator().next())
                    .hasMessageContaining("NULL key is not supported for JSON_OBJECTAGG");

            TestBatchSqlConnector.create(sqlService, "m1", asList("k", "v"), asList(VARCHAR, VARCHAR),
                    singletonList(new String[]{null, "v1"}));

            // null column value for key
            assertThatThrownBy(() -> sqlService.execute("SELECT JSON_OBJECTAGG(k VALUE v) FROM m1").iterator().next())
                    .hasMessageContaining("NULL key is not supported for JSON_OBJECTAGG");
        }

        @Test
        public void test_nullValueLiteral() {
            assertJsonRowsAnyOrder("SELECT JSON_OBJECTAGG('k' VALUE NULL)", singletonList(new Row(json("{\"k\":null}"))));
            // oracle returns {} in this case, but NULL is correct
            assertJsonRowsAnyOrder("SELECT JSON_OBJECTAGG('k' VALUE NULL ABSENT ON NULL)", singletonList(new Row((Object) null)));
            assertJsonRowsAnyOrder("SELECT JSON_OBJECTAGG('k' VALUE NULL ABSENT ON NULL) " +
                    "FROM table(generate_series(1, 1)) " +
                    "WHERE 1=2", singletonList(new Row((Object) null)));
        }

        @Test
        public void test_duplicateKey() {
            TestBatchSqlConnector.create(sqlService, "m", asList("k", "v"), asList(VARCHAR, VARCHAR),
                    asList(new String[]{"k", "v1"},
                            new String[]{"k", "v2"}));

            assertJsonRowsAnyOrder("SELECT JSON_OBJECTAGG(k VALUE v) FROM m", singletonList(new Row(json("{\"k\":\"v1\",\"k\":\"v2\"}"))));
        }

        @Test
        public void test_jsonObjectAgg() {
            String name = createTable();

            assertJsonRowsAnyOrder(
                    "SELECT name, JSON_OBJECTAGG(k VALUE v ABSENT ON NULL) FROM " + name + " WHERE name IS NOT NULL GROUP BY name",
                    asList(
                            new Row("Alice", json("{ \"department\" : \"dep1\", \"job\" : \"job1\", \"description\" : \"desc1\" }")),
                            new Row("Bob", json("{ \"department\" : \"dep2\", \"job\" : \"job2\" }"))
                    )
            );

            assertJsonRowsAnyOrder(
                    "SELECT name, JSON_OBJECTAGG(k VALUE v NULL ON NULL) FROM " + name + " WHERE name IS NOT NULL GROUP BY name",
                    asList(
                            new Row("Alice", json("{ \"department\" : \"dep1\", \"job\" : \"job1\", \"description\" : \"desc1\" }")),
                            new Row("Bob", json("{ \"department\" : \"dep2\", \"job\" : \"job2\", \"description\" : null }"))
                    )
            );
        }

        @Test
        public void test_jsonObjectAgg_whenReturnsNull() {
            String name = createTable();

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

    }
}
