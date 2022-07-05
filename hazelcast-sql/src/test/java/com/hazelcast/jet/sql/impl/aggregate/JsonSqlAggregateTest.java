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
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlService;
import junitparams.JUnitParamsRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;

@RunWith(JUnitParamsRunner.class)
public class JsonSqlAggregateTest extends SqlTestSupport {
    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_jsonArrayAgg_orderedBySameColumn() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "3"},
                new String[]{"Alice", "4"},
                new String[]{null, "6"},
                new String[]{"Alice", "7"},
                new String[]{null, "8"}
        );

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY name ABSENT ON NULL) FROM " + name,
                asList(
                        new Row(new HazelcastJsonValue("[\"Alice\", \"Alice\", \"Alice\", \"Bob\"]"))
                )
        );

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY name NULL ON NULL) FROM " + name,
                asList(
                        new Row(new HazelcastJsonValue("[null, null, \"Alice\", \"Alice\", \"Alice\", \"Bob\"]"))
                )
        );

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY name) FROM " + name,
                asList(
                        new Row(new HazelcastJsonValue("[\"Alice\", \"Alice\", \"Alice\", \"Bob\"]"))
                )
        );
    }

    @Test
    public void test_jsonArrayAgg_orderedByDifferentColumn() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "3"},
                new String[]{"Alice", "4"},
                new String[]{null, "6"},
                new String[]{"Alice", "7"},
                new String[]{null, "8"}
        );

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY distance ABSENT ON NULL) FROM " + name,
                asList(
                        new Row(new HazelcastJsonValue("[\"Alice\", \"Bob\", \"Alice\", \"Alice\"]"))
                )
        );

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY distance NULL ON NULL) FROM " + name,
                asList(
                        new Row(new HazelcastJsonValue("[\"Alice\", \"Bob\", \"Alice\", null, \"Alice\", null]"))
                )
        );

        assertRowsAnyOrder(
                "SELECT JSON_ARRAYAGG(name ORDER BY distance) FROM " + name,
                asList(
                        new Row(new HazelcastJsonValue("[\"Alice\", \"Bob\", \"Alice\", \"Alice\"]"))
                )
        );
    }

    @Test
    public void test_jsonArrayAgg_withGroupBy() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "3"},
                new String[]{"Alice", "4"},
                new String[]{null, "6"},
                new String[]{"Alice", "7"},
                new String[]{null, "8"}
        );

        assertRowsAnyOrder(
                "SELECT name, JSON_ARRAYAGG(distance ORDER BY distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", new HazelcastJsonValue("[1, 4, 7]")),
                        new Row("Bob", new HazelcastJsonValue("[3]")),
                        new Row(null, new HazelcastJsonValue("[6, 8]"))

                )
        );

        assertRowsAnyOrder(
                "SELECT name, JSON_ARRAYAGG(distance ORDER BY distance DESC) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", new HazelcastJsonValue("[7, 4, 1]")),
                        new Row("Bob", new HazelcastJsonValue("[3]")),
                        new Row(null, new HazelcastJsonValue("[8, 6]"))

                )
        );
    }

    @Test
    public void test_jsonArrayAgg_multiple() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "3"},
                new String[]{"Alice", "4"},
                new String[]{null, "6"},
                new String[]{"Alice", "7"},
                new String[]{null, "8"}
        );

        assertRowsAnyOrder(
                "SELECT name, JSON_ARRAYAGG(distance ORDER BY distance DESC), JSON_ARRAYAGG(distance ORDER BY distance ASC) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", new HazelcastJsonValue("[7, 4, 1]"), new HazelcastJsonValue("[1, 4, 7]")),
                        new Row("Bob", new HazelcastJsonValue("[3]"), new HazelcastJsonValue("[3]")),
                        new Row(null, new HazelcastJsonValue("[8, 6]"), new HazelcastJsonValue("[6, 8]"))

                )
        );
    }

    private static String createTable(String[]... values) {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("name", "distance"),
                asList(VARCHAR, INTEGER),
                asList(values)
        );
        return name;
    }
}
