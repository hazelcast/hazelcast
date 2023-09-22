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

package com.hazelcast.jet.sql;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlJsonTypeTest extends SqlJsonTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initializeWithClient(1, null, null);
    }

    @Test
    public void test_insertIntoExistingMap() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        createMapping("test", "bigint", "json");

        test.put(1L, json("[1,2,3]"));
        assertRowsAnyOrder("SELECT * FROM test" ,
                rows(2, 1L, json("[1,2,3]")));

        execute("INSERT INTO test VALUES (2, CAST('[4,5,6]' AS JSON))");
        assertRowsAnyOrder("SELECT * FROM test" ,
                rows(2, 1L, json("[1,2,3]"), 2L, json("[4,5,6]")));

        execute("DELETE FROM test WHERE __key = 1");
        assertRowsAnyOrder("SELECT * FROM test" ,
                rows(2, 2L, json("[4,5,6]")));
    }

    @Test
    public void test_sinkIntoExistingMap() {
        createMapping("test", "bigint", "json");

        execute("INSERT INTO test VALUES (1, CAST('[1,2,3]' AS JSON))");
        assertRowsAnyOrder("SELECT * FROM test" ,
                rows(2, 1L, json("[1,2,3]")));

        execute("SINK INTO test SELECT 1, CAST('[4,5,6]' AS JSON)");
        assertRowsAnyOrder("SELECT * FROM test" ,
                rows(2, 1L, json("[4,5,6]")));
    }

    @Test
    public void test_client_useJsonValue() {
        createMapping(client(), "test", "bigint", "json");

        executeClient("INSERT INTO test VALUES (1, CAST('[4,5,6]' AS JSON))");
        assertRowsAnyOrder(client(),
                "SELECT * FROM test WHERE __key=1" ,
                rows(2, 1L, json("[4,5,6]")));

        executeClient("INSERT INTO test VALUES (2, ?)", json("[7,8,9]"));
        assertRowsAnyOrder(client(),
                "SELECT * FROM test WHERE __key=2",
                rows(2,
                        2L, json("[7,8,9]")
                ));

        // both arguments JSON - test for https://github.com/hazelcast/hazelcast/issues/20303
        executeClient("INSERT INTO test VALUES (3, CAST('[10,11,12]' AS JSON)), (4, ?)", json("[13,14,15]"));
        assertRowsAnyOrder(client(),
                "SELECT * FROM test WHERE __key>=3",
                rows(2,
                        3L, json("[10,11,12]"),
                        4L, json("[13,14,15]")
                ));

        // one argument VARCHAR - test for https://github.com/hazelcast/hazelcast/issues/20303
        executeClient("INSERT INTO test VALUES (5, '[16,17,18]'), (6, ?)", json("[19,20,21]"));
        assertRowsAnyOrder(client(),
                "SELECT * FROM test WHERE __key>=5",
                rows(2,
                        5L, json("[16,17,18]"),
                        6L, json("[19,20,21]")
                ));
    }

    @Test
    public void test_client_useStringValue() {
        createMapping(client(), "test", "bigint", "json");

        final IMap<Long, HazelcastJsonValue> test = client().getMap("test");
        test.put(1L, json("[1,2,3]"));

        executeClient("INSERT INTO test VALUES (2, '[4,5,6]')");
        assertRowsAnyOrder(client(),
                "SELECT * FROM test" ,
                rows(2, 1L, json("[1,2,3]"), 2L, json("[4,5,6]")));

        executeClient("INSERT INTO test VALUES (3, ?)", "[7,8,9]");
        assertRowsAnyOrder(client(),
                "SELECT * FROM test" ,
                rows(2,
                        1L, json("[1,2,3]"),
                        2L, json("[4,5,6]"),
                        3L, json("[7,8,9]")
                ));
    }


    @Test
    // test for https://github.com/hazelcast/hazelcast/issues/22671
    public void test_jsonEqualComparedAsVarchar() {
        // for the `=` operator, we convert JSON operands to VARCHAR
        assertRowsAnyOrder("SELECT " +
                        // json-json not equal
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) = CAST('{\"f2\":43, \"f1\":42}' AS JSON)," +
                        // json-json equal
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) = CAST('{\"f1\":42, \"f2\":43}' AS JSON)," +
                        // json-varchar not equal
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) = '{\"f2\":43, \"f1\":42}'," +
                        "'{\"f1\":42, \"f2\":43}' = CAST('{\"f2\":43, \"f1\":42}' AS JSON)," +
                        // json-varchar equal
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) = '{\"f1\":42, \"f2\":43}'," +
                        "'{\"f1\":42, \"f2\":43}' = CAST('{\"f1\":42, \"f2\":43}' AS JSON)",
                rows(6, false, true, false, false, true, true));
    }

    @Test
    // test for https://github.com/hazelcast/hazelcast/issues/22671
    public void test_jsonNotEqualComparedAsVarchar() {
        // for the `!=` operator, we convert JSON operands to VARCHAR
        assertRowsAnyOrder("SELECT " +
                        // json-json not equal
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) != CAST('{\"f2\":43, \"f1\":42}' AS JSON)," +
                        // json-json equal
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) != CAST('{\"f1\":42, \"f2\":43}' AS JSON)," +
                        // json-varchar not equal
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) != '{\"f2\":43, \"f1\":42}'," +
                        "'{\"f1\":42, \"f2\":43}' != CAST('{\"f2\":43, \"f1\":42}' AS JSON)," +
                        // json-varchar equal
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) != '{\"f1\":42, \"f2\":43}'," +
                        "'{\"f1\":42, \"f2\":43}' != CAST('{\"f1\":42, \"f2\":43}' AS JSON)",
                rows(6, true, false, true, true, false, false));
    }

    @Test
    // test for https://github.com/hazelcast/hazelcast/issues/22671
    public void test_jsonLessComparedAsVarchar() {
        // for the `<` operator, we convert JSON operands to VARCHAR
        // other relative comparison operators should behave the same
        assertRowsAnyOrder("SELECT " +
                        // json-json not equal
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) < CAST('{\"f2\":43, \"f1\":42}' AS JSON)," +
                        // json-json equal
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) < CAST('{\"f1\":42, \"f2\":43}' AS JSON)," +
                        // json-varchar not equal
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) < '{\"f2\":43, \"f1\":42}'," +
                        "'{\"f1\":42, \"f2\":43}' < CAST('{\"f2\":43, \"f1\":42}' AS JSON)," +
                        // json-varchar equal
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) < '{\"f1\":42, \"f2\":43}'," +
                        "'{\"f1\":42, \"f2\":43}' < CAST('{\"f1\":42, \"f2\":43}' AS JSON)",
                rows(6, true, false, true, true, false, false));
    }

    @Test
    public void test_jsonLengthNotSupported() {
        assertThatThrownBy(() -> execute("SELECT " +
                        "LENGTH(CAST('{\"f1\":42, \"f2\":43}' AS JSON))"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageEndingWith("Cannot apply 'LENGTH' function to [JSON] (consider adding an explicit CAST)");
    }

    @Test
    public void test_jsonSubstringNotSupported() {
        assertThatThrownBy(() -> execute("SELECT " +
                "SUBSTRING(CAST('{\"f1\":42, \"f2\":43}' AS JSON) FROM 1 FOR 5)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageEndingWith("Cannot apply 'SUBSTRING' function to [JSON, INTEGER, INTEGER] (consider adding an explicit CAST)");
    }

    @Test
    public void test_jsonConcat() {
        // concat has more permissive conversions than other functions
        assertRowsAnyOrder("SELECT " +
                "CAST('{\"f1\":42, \"f2\":43}' AS JSON) || CAST('suffix' AS VARCHAR)",
                rows(1, "{\"f1\":42, \"f2\":43}suffix"));

        assertRowsAnyOrder("SELECT " +
                        "CAST('{\"f1\":42, \"f2\":43}' AS VARCHAR) || CAST('suffix' AS JSON)",
                rows(1, "{\"f1\":42, \"f2\":43}suffix"));

        assertRowsAnyOrder("SELECT " +
                        "CAST('{\"f1\":42, \"f2\":43}' AS JSON) || CAST('suffix' AS JSON)",
                rows(1, "{\"f1\":42, \"f2\":43}suffix"));

    }

    public void execute(final String sql, final Object... arguments) {
        instance().getSql().execute(sql, arguments);
    }

    public void executeClient(final String sql, final Object... arguments) {
        client().getSql().execute(sql, arguments);
    }
}
