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
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlExpandViewTest extends SqlTestSupport {
    private static final String MAP_NAME = "map";
    private IMap<Integer, Integer> map;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void setUp() throws Exception {
        map = instance().getMap(MAP_NAME);
        createMapping(MAP_NAME, int.class, int.class);
        map.put(1, 1);
    }

    @Test
    public void test_simpleView() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);

        assertRowsAnyOrder("SELECT * FROM v", Collections.singletonList(new Row(1, 1)));
    }

    @Test
    public void when_circularViews_then_fails() {
        instance().getSql().execute("CREATE VIEW v1 AS SELECT * FROM " + MAP_NAME);
        instance().getSql().execute("CREATE VIEW v2 AS SELECT * FROM v1");
        instance().getSql().execute("CREATE OR REPLACE VIEW v1 AS SELECT * FROM v2");

        assertThatThrownBy(() -> instance().getSql().execute("SELECT * FROM v1"))
                .hasMessageContaining("Cycle detected in view references");
    }

    @Test
    public void test_viewWithDistinctSelect() {
        map.put(1, 1);

        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);

        assertRowsAnyOrder("SELECT DISTINCT * FROM v", singletonList(new Row(1, 1)));
    }

    @Test
    public void test_viewWithinWhereClause() {
        map.put(1, 1);

        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);

        assertRowsAnyOrder("SELECT * FROM " + MAP_NAME + " WHERE " +
                        "EXISTS (SELECT * FROM v WHERE __key = " + MAP_NAME + ".__key)",
                singletonList(new Row(1, 1)));

        assertRowsAnyOrder("SELECT * FROM " + MAP_NAME + " WHERE " +
                        "EXISTS (SELECT * FROM v WHERE v.__key = " + MAP_NAME + ".__key)",
                singletonList(new Row(1, 1)));
    }

    @Test
    public void when_viewAfterMappingRemovedIsExpanded_then_fails() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);
        instance().getSql().execute("DROP MAPPING " + MAP_NAME);

        assertThatThrownBy(() -> instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME))
                .hasMessageContaining("Object '" + MAP_NAME + "' not found, did you forget to CREATE MAPPING?");
        assertThatThrownBy(() -> instance().getSql().execute("SELECT * FROM v"))
                .hasMessageContaining("Object '" + MAP_NAME + "' not found within 'hazelcast.public', did you forget to CREATE MAPPING? " +
                        "If you want to use");
    }

    @Test
    public void test_viewWithStreamingQuery() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM TABLE(GENERATE_STREAM(10))");

        assertTipOfStream("SELECT * FROM v", rows(1, 0L, 1L, 2L));
        assertRowsAnyOrder("SELECT * FROM v LIMIT 1", rows(1, 0L));

        assertThatThrownBy(() -> instance().getSql().execute("SELECT * FROM v ORDER BY 1"))
                .hasMessageContaining("Sorting is not supported for a streaming query");

        assertThatThrownBy(() -> instance().getSql().execute("SELECT MAX(v) FROM v"))
                .hasMessageContaining("Streaming aggregation is supported only for window aggregation, with imposed watermark order " +
                        "(see TUMBLE/HOP and IMPOSE_ORDER functions)");
    }

    @Test
    public void when_dml_then_fail() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);

        assertThatThrownBy(() -> instance().getSql().execute("insert into v values(42, 43)"))
                .hasMessageContaining("DML operations not supported for views");
        assertThatThrownBy(() -> instance().getSql().execute("update v set this=44 where __key=42"))
                .hasMessageContaining("DML operations not supported for views");
        assertThatThrownBy(() -> instance().getSql().execute("delete from v where __key=42"))
                .hasMessageContaining("DML operations not supported for views");
    }

    @Test
    public void test_referencedViewChanged() {
        // We create a view v2 as reading from v1, and then change v1.
        // This should be reflected when querying v2 later.
        instance().getSql().execute("CREATE VIEW v1 AS SELECT __key FROM " + MAP_NAME);
        instance().getSql().execute("CREATE VIEW v2 AS SELECT __key FROM v1");
        instance().getSql().execute("CREATE or replace VIEW v1 AS SELECT 'key=' || __key __key FROM " + MAP_NAME);

        assertRowsAnyOrder("select * from v2", rows(1, "key=1"));
    }

    @Test
    public void test_fullyQualifiedViewName() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);

        List<Row> expectedRows = singletonList(new Row(1, 1));
        assertRowsAnyOrder("SELECT * FROM v", expectedRows);
        assertRowsAnyOrder("SELECT * FROM public.v", expectedRows);
        assertRowsAnyOrder("SELECT * FROM hazelcast.public.v", expectedRows);
    }

    @Test
    public void when_viewIsExpandedWithQueryFilter() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);

        assertRowsAnyOrder("SELECT * FROM v WHERE __key > 1", emptyList());
    }

    @Test
    public void when_viewIsExpandedWithViewFilter() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME + " WHERE __key > 1");

        assertRowsAnyOrder("SELECT * FROM v", emptyList());
    }

    @Test
    public void when_viewIsExpandedWithQueryProjection() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);

        assertRowsAnyOrder("SELECT this FROM v", singletonList(new Row(1)));
    }

    @Test
    public void when_viewIsExpandedWithViewProjection() {
        instance().getSql().execute("CREATE VIEW v AS SELECT this FROM " + MAP_NAME);

        assertRowsAnyOrder("SELECT * FROM v", singletonList(new Row(1)));
    }

    @Test
    public void when_viewIsExpandedWithJoin() {
        final String MAP_NAME_2 = "map2";
        final IMap<Integer, Integer> map2 = instance().getMap(MAP_NAME_2);
        createMapping("map2", Integer.class, Integer.class);
        map2.put(1, 1);

        final String sql = "CREATE VIEW v AS SELECT * FROM " + MAP_NAME
                + " INNER JOIN " + MAP_NAME_2 + " ON " + MAP_NAME_2 + ".__key = " + MAP_NAME + " .__key";

        instance().getSql().execute(sql);

        assertRowsAnyOrder("SELECT * FROM v", singletonList(new Row(1, 1, 1, 1)));
    }

    @Test
    public void when_viewIsExpandedWithCrossProduct() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);
        assertRowsAnyOrder("SELECT * FROM v v1 CROSS JOIN v v2", singletonList(new Row(1, 1, 1, 1)));
    }

    @Test
    public void when_viewIsExpandedAsJoinRHS() {
        final String MAP_NAME_2 = "map2";
        final IMap<Integer, Integer> map2 = instance().getMap(MAP_NAME_2);
        createMapping("map2", Integer.class, Integer.class);
        map2.put(1, 1);

        final String sql = "CREATE VIEW v AS SELECT * FROM " + MAP_NAME;

        instance().getSql().execute(sql);

        assertRowsAnyOrder(
                "SELECT * FROM " + MAP_NAME_2 + " JOIN v ON v.__key = " + MAP_NAME_2 + ".__key",
                singletonList(new Row(1, 1, 1, 1))
        );
    }

    @Test
    public void when_viewIsExpandedWithOrdering() {
        map.put(2, 2);

        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME + " ORDER BY __key DESC");
        instance().getSql().execute("CREATE VIEW vv AS SELECT * FROM " + MAP_NAME + " ORDER BY __key");

        assertRowsAnyOrder("SELECT * FROM v", asList(new Row(2, 2), new Row(1, 1)));
        assertRowsAnyOrder("SELECT * FROM vv", asList(new Row(1, 1), new Row(2, 2)));
    }

    @Test
    public void when_viewIsExpandedWithUnionAll() {
        final String MAP_NAME_2 = "map2";
        final IMap<Integer, Integer> map2 = instance().getMap(MAP_NAME_2);
        createMapping("map2", Integer.class, Integer.class);
        map2.put(2, 2);

        final String sql = "CREATE VIEW v AS "
                + "(SELECT * FROM " + MAP_NAME + " UNION ALL "
                + "SELECT * FROM " + MAP_NAME_2 + ")";

        instance().getSql().execute(sql);

        assertRowsAnyOrder("SELECT * FROM v", asList(new Row(1, 1), new Row(2, 2)));
    }

    @Test
    public void when_viewIsExpandedWithValues() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM ( VALUES(1, 1), (2, 2) )");
        assertRowsAnyOrder("SELECT * FROM v", asList(
                new Row((byte) 1, (byte) 1),
                new Row((byte) 2, (byte) 2)
        ));
    }

    @Ignore("SCALAR QUERY not supported")
    @Test
    public void when_viewIsExpandedWithValuesAndSubQueryWithin() {
        instance().getSql().execute("CREATE VIEW v AS SELECT 1");
        instance().getSql().execute("CREATE VIEW vv AS SELECT 2");
        instance().getSql().execute("CREATE VIEW vvv AS SELECT * FROM ( VALUES(SELECT * FROM v), (SELECT * FROM vv) )");
        assertRowsAnyOrder("SELECT (SELECT 1 FROM v limit 1) FROM " + MAP_NAME, singletonList(new Row((byte) 1)));
    }

    @Test
    public void when_viewIsExpandedWithAggFunction() {
        instance().getSql().execute("CREATE VIEW v AS SELECT MAX(__key) FROM map");

        assertRowsAnyOrder("SELECT * FROM v", singletonList(new Row(1)));
    }

    @Test
    public void when_viewIsExpandedWithTumbleFunction() {
        String name = createStreamingTable(
                instance().getSql(),
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), null, null),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(10), null, null)
        );

        instance().getSql().execute("CREATE VIEW v " +
                "AS SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND))"
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT window_start, SUM(distance) " +
                        "FROM TABLE(TUMBLE(TABLE v, DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY window_start",
                asList(
                        new Row(timestampTz(0L), 1L),
                        new Row(timestampTz(2L), 2L)
                )
        );
    }

    @Test
    public void when_viewIsExpandedWithGroupByAndHaving() {
        map.put(2, 2);
        map.put(3, 3);
        instance().getSql().execute("CREATE VIEW v AS SELECT this FROM map GROUP BY (this) HAVING AVG(this) = 2");

        assertRowsAnyOrder("SELECT * FROM v", singletonList(new Row(2)));
    }

    @Test
    public void when_viewIsExpandedWithJsonFunctions() {
        createMapping("test", "bigint", "json");
        instance().getSql().execute("INSERT INTO test VALUES (1, '[1,2,3]')");
        instance().getSql().execute("INSERT INTO test VALUES (2, '[4,5,6]')");

        instance().getSql().execute("CREATE VIEW v1 AS SELECT JSON_VALUE(this, '$[1]' "
                + "RETURNING BIGINT NULL ON EMPTY NULL ON ERROR) FROM test");
        instance().getSql().execute("CREATE VIEW v2 AS SELECT JSON_QUERY(this, '$[1]' "
                + "WITH CONDITIONAL WRAPPER EMPTY OBJECT ON EMPTY EMPTY OBJECT ON ERROR) FROM test");

        assertRowsAnyOrder("SELECT * FROM v1", asList(new Row(2L), new Row(5L)));
        assertRowsAnyOrder("SELECT * FROM v2", asList(
                new Row(new HazelcastJsonValue("2")),
                new Row(new HazelcastJsonValue("5"))
        ));
    }

    @Test
    public void when_doubleViewIsExpandedDuringQuery() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME + " WHERE __key = 1");
        instance().getSql().execute("CREATE VIEW vv AS SELECT * FROM v");

        assertRowsAnyOrder("SELECT * FROM vv", singletonList(new Row(1, 1)));
    }

    @Test
    public void when_doubleViewIsExpandedDuringQueryWithProjection() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME + " WHERE __key = 1");
        instance().getSql().execute("CREATE VIEW vv AS SELECT __key FROM v");

        assertRowsAnyOrder("SELECT * FROM vv", singletonList(new Row(1)));
    }

    @Test
    public void when_doubleViewIsExpandedDuringQueryWithStringConcat() {
        IMap<String, String> map2;
        map2 = instance().getMap("map2");
        createMapping("map2", String.class, String.class);
        map2.put("a", "b");

        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM map2");
        instance().getSql().execute("CREATE VIEW vv AS SELECT __key || this FROM v");

        assertRowsAnyOrder("SELECT * FROM vv", singletonList(new Row("ab")));
    }

    @Test
    public void when_doubleViewIsExpandedDuringQueryWithFunctionProjections() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME + " WHERE this = 1");
        instance().getSql().execute("CREATE VIEW vv AS SELECT MAX(this) FROM v");
        instance().getSql().execute("CREATE VIEW vvv AS SELECT COUNT(this) FROM v");

        assertRowsAnyOrder("SELECT * FROM vv", rows(1, 1));
        assertRowsAnyOrder("SELECT * FROM vvv", rows(1, 1L));
    }

    @Test
    public void when_tripleViewIsExpandedDuringQuery() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME + " WHERE __key = 1");
        instance().getSql().execute("CREATE VIEW vv AS SELECT * FROM v");
        instance().getSql().execute("CREATE VIEW vvv AS SELECT * FROM vv");

        assertRowsAnyOrder("SELECT * FROM vvv", singletonList(new Row(1, 1)));
    }

    @Test
    public void when_doubleViewWithQueryFilter() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME + " WHERE __key > 0");
        instance().getSql().execute("CREATE VIEW vv AS SELECT * FROM v");

        assertRowsAnyOrder("SELECT * FROM vv WHERE __key = 1", Collections.singletonList(new Row(1, 1)));
    }

    @Test
    public void when_doubleViewExpandedAndProjectedInNestedQuery() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM (SELECT __key FROM " + MAP_NAME + ") WHERE __key = 1");
        instance().getSql().execute("CREATE VIEW vv AS SELECT * FROM v");

        assertRowsAnyOrder("SELECT * FROM vv WHERE __key = 1", singletonList(new Row(1)));
    }

    private static String createStreamingTable(SqlService service, Object[]... values) {
        String name = randomName();
        TestStreamSqlConnector.create(
                service,
                name,
                asList("ts", "name", "distance"),
                asList(TIMESTAMP_WITH_TIME_ZONE, VARCHAR, INTEGER),
                values
        );
        return name;
    }
}
