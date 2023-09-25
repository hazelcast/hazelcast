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

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.ResultIterator.HasNextResult;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("resource")
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlUnionTest extends SqlTestSupport {
    private IMap<Integer, Person> map1;
    private IMap<Integer, Person> map2;
    private IMap<Integer, Person> map3;

    private final List<Row> expected = new ArrayList<>();

    @BeforeClass
    public static void beforeClass() {
        initialize(3, null);
    }

    @Before
    public void before() throws Exception {
        map1 = instance().getMap("map1");
        map2 = instance().getMap("map2");
        map3 = instance().getMap("map3");
    }

    @Test
    public void baseUnionTest() {
        createMapping("map1", Integer.class, Person.class);
        createMapping("map2", Integer.class, Person.class);

        for (int i = 0; i < 50; ++i) {
            map1.put(i, new Person(i, "ABC" + i));
            map2.put(i, new Person(i, "ABC" + i));
            expected.add(new Row(i, i, "ABC" + i));
        }
        String sql = "(SELECT * FROM map1) UNION (SELECT * FROM map2)";
        assertRowsAnyOrder(sql, expected);
    }

    @Test
    public void multipleUnionTest() {
        createMapping("map1", Integer.class, Person.class);
        createMapping("map2", Integer.class, Person.class);
        createMapping("map3", Integer.class, Person.class);

        for (int i = 0; i < 50; ++i) {
            map1.put(i, new Person(i, "ABC" + i));
            map2.put(i, new Person(i, "ABC" + i));
            map3.put(i, new Person(i, "ABC" + i));
            expected.add(new Row(i, i, "ABC" + i));
        }

        String sql = "(SELECT * FROM map1) UNION (SELECT * FROM map2) UNION (SELECT * FROM map3)";
        assertRowsAnyOrder(sql, expected);
    }

    @Test
    public void baseUnionAllTest() {
        createMapping("map1", Integer.class, Person.class);
        createMapping("map2", Integer.class, Person.class);

        for (int i = 0; i < 50; ++i) {
            map1.put(i, new Person(i, "ABC" + i));
            map2.put(i, new Person(i, "ABC" + i));
            expected.add(new Row(i, i, "ABC" + i));
            expected.add(new Row(i, i, "ABC" + i));
        }
        String sql = "(SELECT * FROM map1) UNION ALL (SELECT * FROM map2)";
        assertRowsAnyOrder(sql, expected);
    }

    @Test
    public void multipleUnionAllTest() {
        createMapping("map1", Integer.class, Person.class);
        createMapping("map2", Integer.class, Person.class);
        createMapping("map3", Integer.class, Person.class);

        for (int i = 0; i < 50; ++i) {
            map1.put(i, new Person(i, "ABC" + i));
            map2.put(i, new Person(i, "ABC" + i));
            map3.put(i, new Person(i, "ABC" + i));
            expected.addAll(
                    asList(new Row(i, i, "ABC" + i), new Row(i, i, "ABC" + i), new Row(i, i, "ABC" + i))
            );
        }

        String sql = "(SELECT * FROM map1) UNION ALL (SELECT * FROM map2) UNION ALL (SELECT * FROM map3)";
        assertRowsAnyOrder(sql, expected);
    }

    @Test
    public void baseUnionColumnCountMismatchTest() {
        createMapping("map1", Integer.class, Person.class);
        createMapping("map2", Integer.class, Integer.class);

        assertThatThrownBy(() -> instance().getSql().execute("(SELECT * FROM map1) UNION (SELECT __key FROM map2)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Column count mismatch in UNION");

        assertThatThrownBy(() -> instance().getSql().execute("(SELECT __key FROM map1) UNION (SELECT * FROM map2)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Column count mismatch in UNION");
    }

    @Test
    public void baseUnionTypeMismatchErrorTest() {
        instance().getMap("map4");
        createMapping("map1", Integer.class, Person.class);
        createMapping("map4", Integer.class, Integer.class);

        assertThatThrownBy(() -> instance().getSql().execute("(SELECT name FROM map1) UNION (SELECT this FROM map4)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Cannot infer return type for UNION");

        assertThatThrownBy(() -> instance().getSql().execute("(SELECT this FROM map4) UNION (SELECT name FROM map1)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Cannot infer return type for UNION");
    }

    @Test
    public void baseUnionAllColumnCountMismatchTest() {
        createMapping("map1", Integer.class, Person.class);
        createMapping("map2", Integer.class, Person.class);

        assertThatThrownBy(() -> instance().getSql().execute("(SELECT * FROM map1) UNION ALL (SELECT __key FROM map2)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Column count mismatch in UNION");

        assertThatThrownBy(() -> instance().getSql().execute("(SELECT __key FROM map1) UNION ALL (SELECT * FROM map2)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Column count mismatch in UNION");
    }

    @Test
    public void baseUnionAllTypeMismatchErrorTest() {
        instance().getMap("map4");
        createMapping("map1", Integer.class, Person.class);
        createMapping("map4", Integer.class, Integer.class);

        assertThatThrownBy(() -> instance().getSql().execute("(SELECT name FROM map1) UNION ALL (SELECT this FROM map4)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Cannot infer return type for UNION ALL");

        assertThatThrownBy(() -> instance().getSql().execute("(SELECT this FROM map4) UNION ALL (SELECT name FROM map1)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Cannot infer return type for UNION ALL");
    }

    @Test
    public void test_unionOfTableScanAndValues() {
        // test for https://github.com/hazelcast/hazelcast/issues/19772
        map1 = instance().getMap("map1");

        createMapping("map1", Integer.class, Person.class);

        for (int i = 0; i < 50; ++i) {
            map1.put(i, new Person(i, "ABC" + i));
            expected.add(new Row((byte) 1));
        }
        expected.add(new Row((byte) 1));
        String sql = "SELECT 1 FROM (values(1)) UNION ALL SELECT 1 FROM map1";
        assertRowsAnyOrder(sql, expected);
    }

    @Test
    public void watermarkedStreamUnionTest() {
        String name = createTable(
                row(timestampTz(10L), 1),
                row(timestampTz(12L), 2),
                row(timestampTz(14L), 3),
                row(timestampTz(0L), 4)
        );

        String sql = "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.001' SECOND))";

        String query = sql + " UNION " + sql;
        // Internally, UNION query would be converted into DISTINCT aggregation.
        // Non-windowed streaming aggregation is not supported.
        assertThatThrownBy(() -> instance().getSql().execute(query))
                .hasMessageContaining("Streaming aggregation is supported only for window aggregation");
    }

    @Test
    public void watermarkedStreamUnionAllTest() {
        String name = createTable(
                row(timestampTz(100L), 1),
                row(timestampTz(120L), 2),
                row(timestampTz(140L), 3),
                row(timestampTz(0L), 4)
        );

        String sql = "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.001' SECOND))";

        expected.add(new Row(timestampTz(100L), 1));
        expected.add(new Row(timestampTz(100L), 1));
        expected.add(new Row(timestampTz(120L), 2));
        expected.add(new Row(timestampTz(120L), 2));
        expected.add(new Row(timestampTz(140L), 3));
        expected.add(new Row(timestampTz(140L), 3));

        SqlResult result = instance().getSql().execute(sql + " UNION ALL " + sql);
        ResultIterator<SqlRow> iterator = (ResultIterator<SqlRow>) result.iterator();
        List<Row> actualRows = new ArrayList<>();
        assertTrueEventually(() -> {
                    while (iterator.hasNext(50, TimeUnit.MILLISECONDS) == HasNextResult.YES) {
                        actualRows.add(new Row(iterator.next()));
                    }
                    if (actualRows.size() == 7 && expected.size() < 7) {
                        // Because we drop late items after union all (thanks to UnionDropLateItemsTransposeRule),
                        // it can happen that one of the late items makes it through. But both will never make it.
                        expected.add(new Row(timestampTz(0L), 4));
                    }
                    assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expected);
                },
                5);
    }

    @Test
    public void prunableUnionAllTest() {
        String map1Name = randomName();
        String map2Name = randomName();
        instance().getConfig().addMapConfig(
                new MapConfig(map1Name).setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("id")
                )));
        instance().getConfig().addMapConfig(
                new MapConfig(map2Name).setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("id")
                )));

        IMap<Person, String> prunableMap1 = instance().getMap(map1Name);
        IMap<Person, String> prunableMap2 = instance().getMap(map2Name);

        createMapping(map1Name, Person.class, String.class);
        createMapping(map2Name, Person.class, String.class);

        for (int i = 0; i < 5; ++i) {
            prunableMap1.put(new Person(i, "ABC" + i), "" + i);
            prunableMap2.put(new Person(i, "ABC" + i), "" + i);
        }

        String sql = "(SELECT this FROM " + map1Name + " WHERE id = 1) " +
                "UNION ALL " +
                "(SELECT this FROM " + map2Name + " WHERE id = 1)";
        expected.add(new Row("1"));
        expected.add(new Row("1"));

        assertRowsAnyOrder(sql, expected);
    }

    @Test
    public void prunableUnionTest() {
        // Note: it is a test for the future implementation of prunable Aggregation.
        //  Union converts to UnionAll + Aggregation, and  prunable Aggregation implementor
        //  easily may miss that fact during testing.
        //  Right now it just a execution check test.
        // https://hazelcast.atlassian.net/browse/HZ-2796
        String map1Name = randomName();
        String map2Name = randomName();
        instance().getConfig().addMapConfig(
                new MapConfig(map1Name).setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("id")
                )));
        instance().getConfig().addMapConfig(
                new MapConfig(map2Name).setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("id")
                )));

        IMap<Person, String> prunableMap1 = instance().getMap(map1Name);
        IMap<Person, String> prunableMap2 = instance().getMap(map2Name);

        createMapping(map1Name, Person.class, String.class);
        createMapping(map2Name, Person.class, String.class);

        for (int i = 0; i < 5; ++i) {
            prunableMap1.put(new Person(i, "ABC" + i), "" + i);
            prunableMap2.put(new Person(i, "ABC" + i), "" + i);
        }

        String sql = "(SELECT this FROM " + map1Name + " WHERE id = 1) " +
                "UNION " +
                "(SELECT this FROM " + map2Name + " WHERE id = 1)";

        expected.add(new Row("1"));

        assertRowsAnyOrder(sql, expected);
    }

    @Test
    public void prunableSelfUnionAllTest() {
        instance().getConfig().addMapConfig(
                new MapConfig("pMap1").setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("id")
                )));

        IMap<Person, String> prunableMap1 = instance().getMap("pMap1");

        createMapping("pMap1", Person.class, String.class);

        for (int i = 0; i < 5; ++i) {
            prunableMap1.put(new Person(i, "ABC" + i), "" + i);
        }

        String sql = "(SELECT this FROM pMap1 WHERE id = 1) UNION ALL (SELECT this FROM pMap1 WHERE id = 1)";
        expected.add(new Row("1"));
        expected.add(new Row("1"));

        assertRowsAnyOrder(sql, expected);
    }

    @Test
    public void prunableSelfUnionTest() {
        // Note: it is a test for the future implementation of prunable Aggregation.
        //  Union converts to UnionAll + Aggregation, and  prunable Aggregation implementor
        //  easily may miss that fact during testing.
        //  Right now it just a execution check test.
        // JIRA issue : https://hazelcast.atlassian.net/browse/HZ-2796
        instance().getConfig().addMapConfig(
                new MapConfig("pMap1").setPartitioningAttributeConfigs(List.of(
                        new PartitioningAttributeConfig("id")
                )));

        IMap<Person, String> prunableMap1 = instance().getMap("pMap1");

        createMapping("pMap1", Person.class, String.class);
        createMapping("pMap2", Person.class, String.class);

        for (int i = 0; i < 5; ++i) {
            prunableMap1.put(new Person(i, "ABC" + i), "" + i);
        }

        String sql = "(SELECT this FROM pMap1 WHERE id = 1) UNION (SELECT this FROM pMap1 WHERE id = 1)";
        expected.add(new Row("1"));

        assertRowsAnyOrder(sql, expected);
    }

    private static String createTable(Object[]... values) {
        String name = randomName();
        TestStreamSqlConnector.create(
                instance().getSql(),
                name,
                asList("ts", "id"),
                asList(TIMESTAMP_WITH_TIME_ZONE, INTEGER),
                values
        );
        return name;
    }
}
