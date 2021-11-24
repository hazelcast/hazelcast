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

import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.schema.view.View;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void when_simpleViewIsExpanded() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);

        assertRowsAnyOrder("SELECT * FROM v", Collections.singletonList(new Row(1, 1)));
    }

    @Test
    public void when_incorrectQueryView_then_throws() {
        assertThatThrownBy(() -> instance().getSql().execute("CREATE VIEW v AS SELECT -"))
                .hasMessageContaining("Encountered \"<EOF>\" at line 1");
    }

    @Test
    public void when_simpleViewAfterMappingRemovedIsExpanded_then_throws() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);
        instance().getSql().execute("DROP MAPPING " + MAP_NAME);

        assertThatThrownBy(() -> instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME))
                .hasMessageContaining("Object '" + MAP_NAME + "' not found, did you forget to CREATE MAPPING?");
    }

    @Test
    public void when_simpleViewWithStreamingQueryIsExpanded() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM TABLE(GENERATE_SERIES(-5, 5, 5))");

        assertRowsAnyOrder("SELECT * FROM v", asList(new Row(-5), new Row(0), new Row(5)));
    }

    @Ignore
    @Test
    public void when_circularViewsResolvedCorrectly() {
        instance().getSql().execute("CREATE VIEW v1 AS SELECT * FROM " + MAP_NAME);
        instance().getSql().execute("CREATE VIEW v2 AS SELECT * FROM v1");
        instance().getSql().execute("CREATE OR REPLACE VIEW v1 AS SELECT * FROM v2");

        assertRowsAnyOrder("SELECT * FROM v1", singletonList(new Row(1, 1)));
    }

    @Test
    public void when_fullSchemaViewIsExpanded() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);

        List<Row> expectedRows = singletonList(new Row(1, 1));
        assertRowsAnyOrder("SELECT * FROM v", expectedRows);
        assertRowsAnyOrder("SELECT * FROM public.v", expectedRows);
        assertRowsAnyOrder("SELECT * FROM hazelcast.public.v", expectedRows);
    }

    @Test
    public void when_simpleViewIsExpandedWithQueryFilter() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);

        assertRowsAnyOrder("SELECT * FROM v WHERE __key > 1", emptyList());
    }

    @Test
    public void when_simpleViewIsExpandedWithViewFilter() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME + " WHERE __key > 1");

        assertRowsAnyOrder("SELECT * FROM v", emptyList());
    }

    @Test
    public void when_simpleViewIsExpandedWithQueryProjection() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);

        assertRowsAnyOrder("SELECT this FROM v", singletonList(new Row(1)));
    }

    @Test
    public void when_simpleViewIsExpandedWithViewProjection() {
        instance().getSql().execute("CREATE VIEW v AS SELECT this FROM " + MAP_NAME);

        assertRowsAnyOrder("SELECT * FROM v", singletonList(new Row(1)));
    }

    @Test
    public void when_simpleViewIsExpandedWithJoin() {
        final String MAP_NAME_2 = "map2";
        final IMap<Integer, Integer> map2 = instance().getMap(MAP_NAME_2);
        createMapping("map2", Integer.class, Integer.class);
        map2.put(1, 1);

        final String sql = "CREATE VIEW v AS SELECT * FROM " + MAP_NAME
                + " INNER JOIN " + MAP_NAME_2 + " ON " + MAP_NAME_2 + ".__key = " + MAP_NAME + " .__key";

        instance().getSql().execute(sql);

        assertRowsAnyOrder("SELECT * FROM v", singletonList(new Row(1, 1, 1, 1)));
    }

    @Ignore("Sub-query not supported on the right side of a (LEFT) JOIN or the left side of a RIGHT JOIN")
    @Test
    public void when_simpleViewIsExpandedAsJoinRHS() {
        final String MAP_NAME_2 = "map2";
        final IMap<Integer, Integer> map2 = instance().getMap(MAP_NAME_2);
        createMapping("map2", Integer.class, Integer.class);
        map2.put(1, 1);

        final String sql = "CREATE VIEW v AS SELECT * FROM " + MAP_NAME;

        instance().getSql().execute(sql);

        assertRowsAnyOrder(
                "SELECT * FROM " + MAP_NAME_2 + " JOIN v ON " + MAP_NAME + ".__key = " + MAP_NAME_2 + ".__key = 1",
                singletonList(new Row(1, 1, 1, 1))
        );
    }

    @Test
    public void when_simpleViewIsExpandedWithOrdering() {
        map.put(2, 2);

        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME + " ORDER BY __key DESC");

        assertRowsAnyOrder("SELECT * FROM v", asList(new Row(2, 2), new Row(1, 1)));
    }

    @Test
    public void when_simpleViewIsExpandedWithUnionAll() {
        final String MAP_NAME_2 = "map2";
        final IMap<Integer, Integer> map2 = instance().getMap(MAP_NAME_2);
        createMapping("map2", Integer.class, Integer.class);
        map2.put(1, 1);

        final String sql = "CREATE VIEW v AS "
                + "SELECT * FROM " + MAP_NAME + " UNION ALL "
                + "SELECT * FROM " + MAP_NAME_2;

        instance().getSql().execute(sql);

        assertRowsAnyOrder("SELECT * FROM v", asList(new Row(1, 1), new Row(1, 1)));
    }

    @Test
    public void when_simpleViewIsExpandedWithAggFunction() {
        instance().getSql().execute("CREATE VIEW v AS SELECT MAX(__key) FROM map");

        assertRowsAnyOrder("SELECT * FROM v", singletonList(new Row(1)));
    }

    @Test
    public void when_simpleViewIsExpandedWithGroupByAndHaving() {
        map.put(2, 2);
        map.put(3, 3);
        instance().getSql().execute("CREATE VIEW v AS SELECT this FROM map GROUP BY (this) HAVING AVG(this) = 2");

        assertRowsAnyOrder("SELECT * FROM v", singletonList(new Row(2)));
    }

    @Test
    public void when_doubleViewIsExpandedDuringQuery() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME + " WHERE __key = 1");
        instance().getSql().execute("CREATE VIEW vv AS SELECT * FROM v");

        assertRowsAnyOrder("SELECT * FROM vv", singletonList(new Row(1, 1)));
    }

    @Test
    public void when_doubleViewIsNotExpandedDuringViewCreation() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME + " WHERE __key > 1");
        instance().getSql().execute("CREATE VIEW vv AS SELECT * FROM v");

        View vv = (View) instance().getReplicatedMap("__sql.catalog").get("vv");

        assertThat(vv.query()).isEqualTo("SELECT *\n" + "FROM \"hazelcast\".\"public\".\"v\" AS \"v\"");
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
}
