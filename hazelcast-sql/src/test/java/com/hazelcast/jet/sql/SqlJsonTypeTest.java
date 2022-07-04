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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SlowTest.class)
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

    public void execute(final String sql, final Object ...arguments) {
        instance().getSql().execute(sql, arguments);
    }

    public void executeClient(final String sql, final Object ...arguments) {
        client().getSql().execute(sql, arguments);
    }
}
