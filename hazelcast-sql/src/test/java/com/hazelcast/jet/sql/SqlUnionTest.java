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

import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlUnionTest extends SqlTestSupport {
    private IMap<Integer, Person> map1;
    private IMap<Integer, Person> map2;
    private IMap<Integer, Person> map3;

    private List<Row> expected = new ArrayList<>();

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void before() throws Exception {
        map1 = instance().getMap("map1");
        map2 = instance().getMap("map2");
        map3 = instance().getMap("map3");

        createMapping("map1", Integer.class, Person.class);
        createMapping("map2", Integer.class, Person.class);

        for (int i = 0; i < 50; ++i) {
            map1.put(i, new Person(i, "ABC" + i));
            map2.put(i, new Person(i, "ABC" + i));
            expected.add(new Row(i, i, "ABC" + i));
        }
    }

    @Override
    @After
    public void tearDown() {
        map1.clear();
        map2.clear();
        map3.clear();
        expected.clear();
        super.tearDown();
    }

    @Test
    public void baseUnionTest() {
        String sql = "(SELECT * FROM map1) UNION (SELECT * FROM map2)";
        assertRowsAnyOrder(sql, expected);
    }

    @Test
    public void multipleUnionTest() {
        createMapping("map3", Integer.class, Person.class);

        for (int i = 0; i < 50; ++i) {
            map3.put(i, new Person(i, "ABC" + i));
        }
        String sql = "(SELECT * FROM map1) UNION (SELECT * FROM map2) UNION (SELECT * FROM map3)";
        assertRowsAnyOrder(sql, expected);
    }

    @Test
    public void baseUnionAllTest() {
        for (int i = 0; i < 50; ++i) {
            expected.add(new Row(i, i, "ABC" + i));
        }
        String sql = "(SELECT * FROM map1) UNION ALL (SELECT * FROM map2)";
        assertRowsAnyOrder(sql, expected);
    }

    @Test
    public void multipleUnionAllTest() {
        createMapping("map3", Integer.class, Person.class);

        for (int i = 0; i < 50; ++i) {
            map3.put(i, new Person(i, "ABC" + i));
            expected.add(new Row(i, i, "ABC" + i));
            expected.add(new Row(i, i, "ABC" + i));
        }
        String sql = "(SELECT * FROM map1) UNION ALL (SELECT * FROM map2) UNION ALL (SELECT * FROM map3)";
        assertRowsAnyOrder(sql, expected);
    }

    @Test
    public void baseUnionErrorTest() {
        assertThatThrownBy(() -> instance().getSql().execute("(SELECT * FROM map1) UNION (SELECT __key FROM map2)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Column count mismatch in UNION");

        assertThatThrownBy(() -> instance().getSql().execute("(SELECT __key FROM map1) UNION (SELECT * FROM map2)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Column count mismatch in UNION");
    }

    @Test
    public void baseUnionAllErrorTest() {
        assertThatThrownBy(() -> instance().getSql().execute("(SELECT * FROM map1) UNION ALL (SELECT __key FROM map2)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Column count mismatch in UNION");

        assertThatThrownBy(() -> instance().getSql().execute("(SELECT __key FROM map1) UNION ALL (SELECT * FROM map2)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Column count mismatch in UNION");
    }
}
