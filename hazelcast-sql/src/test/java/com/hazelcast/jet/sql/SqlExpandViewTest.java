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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

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
    public void when_simpleViewWithStreamingQueryIsExpanded() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM TABLE(GENERATE_SERIES(-5, 5, 5))");

        assertRowsAnyOrder("SELECT * FROM v", asList(new Row(-5), new Row(0), new Row(5)));
    }

    @Test
    public void when_fullSchemaViewIsExpanded() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME);

        assertRowsAnyOrder("SELECT * FROM hazelcast.public.v", Collections.singletonList(new Row(1, 1)));
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
                + " INNER JOIN " + MAP_NAME_2 + " ON map2.__key = map.__key";

        instance().getSql().execute(sql);

        assertRowsAnyOrder("SELECT * FROM v", singletonList(new Row(1, 1, 1, 1)));
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
    public void when_doubleView() {
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + MAP_NAME + " WHERE __key > 1");
        instance().getSql().execute("CREATE VIEW vv AS SELECT * FROM v");

        assertRowsAnyOrder("SELECT * FROM vv", emptyList());
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

    // hazelcast.public
}