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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExplainStatementTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void test_explainStatementBase() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 10);

        String sql = "EXPLAIN SELECT * FROM map";

        createMapping("map", Integer.class, Integer.class);
        assertRowsAnyOrder(sql, singletonList(
                new Row("FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]])")
        ));

        sql = "EXPLAIN PLAN FOR SELECT * FROM map";
        assertRowsAnyOrder(sql, singletonList(
                new Row("FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]])")
        ));
    }

    @Test
    public void test_explainStatementIndexScan() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 10);
        map.put(2, 10);
        map.put(3, 10);
        map.put(4, 10);
        map.put(5, 10);
        map.addIndex(IndexType.HASH, "this");

        String sql = "EXPLAIN PLAN FOR SELECT * FROM map WHERE this = 10";

        createMapping("map", Integer.class, Integer.class);
        assertRowsAnyOrder(sql, singletonList(
                new Row("IndexScanMapPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]], " +
                        "index=[map_hash_this], indexExp=[=($1, 10)], remainderExp=[null])")
        ));
    }

    @Test
    public void test_explainStatementSortedIndexScan() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        map.addIndex(IndexType.SORTED, "this");

        String sql = "EXPLAIN PLAN FOR SELECT * FROM map ORDER BY this";

        createMapping("map", Integer.class, Integer.class);
        assertRowsAnyOrder(sql, singletonList(
                new Row("IndexScanMapPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]], " +
                        "index=[map_sorted_this], indexExp=[null], remainderExp=[null])")
        ));
    }

    @Test
    public void test_explainStatementOrderedScanBelowUnion() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        String sql = "EXPLAIN PLAN FOR SELECT * FROM map UNION ALL SELECT * FROM map ORDER BY this DESC";

        createMapping("map", Integer.class, Integer.class);
        assertRowsAnyOrder(sql, asList(
                new Row("SortPhysicalRel(sort0=[$1], dir0=[DESC], requiresSort=[true])"),
                new Row("  UnionPhysicalRel(all=[true])"),
                new Row("    FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]])"),
                new Row("    FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]])")
        ));
    }

    @Test
    public void test_explainStatementSelectBelowUnion() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 10);

        String sql = "EXPLAIN PLAN FOR SELECT * FROM map UNION ALL SELECT * FROM map";

        createMapping("map", Integer.class, Integer.class);
        assertRowsAnyOrder(sql, asList(
                new Row("UnionPhysicalRel(all=[true])"),
                new Row("  FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]])"),
                new Row("  FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]])")
        ));
    }

    @Test
    public void test_explainStatementJoin() {
        IMap<Integer, Integer> map1 = instance().getMap("map1");
        map1.put(10, 1);
        IMap<Integer, Person> map2 = instance().getMap("map2");
        map2.put(1, new Person(10, "A"));

        String sql = "EXPLAIN PLAN FOR SELECT map1.__key, map2.name FROM map1 INNER JOIN map2 ON map1.__key = map2.id";

        createMapping("map1", Integer.class, Integer.class);
        createMapping("map2", Integer.class, Person.class);

        assertRowsAnyOrder(sql, asList(
                new Row("ProjectPhysicalRel(__key=[$0], name=[$2])"),
                new Row("  JoinNestedLoopPhysicalRel(condition=[=($0, $1)], joinType=[inner])"),
                new Row("    FullScanPhysicalRel(table=[[hazelcast, public, map1[projects=[0]]]])"),
                new Row("    FullScanPhysicalRel(table=[[hazelcast, public, map2[projects=[1, 2]]]])")
        ));
    }

    @Test
    public void test_explainStatementInsert() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 1);

        String sql = "EXPLAIN PLAN FOR INSERT INTO map VALUES (2, 2)";

        createMapping("map", Integer.class, Integer.class);

        assertRowsAnyOrder(sql, singletonList(
                new Row("InsertMapPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]], values=[{" +
                        "expressions=[[" +
                        "ConstantExpression{type=QueryDataType {family=INTEGER}, value=2}, " +
                        "ConstantExpression{type=QueryDataType {family=INTEGER}, value=2}]]}])"
                )));

        createMapping("map", Integer.class, Integer.class);

        sql = "EXPLAIN PLAN FOR INSERT INTO map VALUES (3, 3), (4, 4)";
        assertRowsAnyOrder(sql, asList(
                new Row("InsertPhysicalRel(" +
                        "table=[[hazelcast, public, map[projects=[0, 1]]]], operation=[INSERT], flattened=[false])"),
                new Row("  ValuesPhysicalRel(values=[{expressions=[" +
                        "[ConstantExpression{type=QueryDataType {family=INTEGER}, value=3}, " +
                        "ConstantExpression{type=QueryDataType {family=INTEGER}, value=3}], " +
                        "[ConstantExpression{type=QueryDataType {family=INTEGER}, value=4}, " +
                        "ConstantExpression{type=QueryDataType {family=INTEGER}, value=4}]]}])")
        ));
    }

    @Test
    public void test_explainStatementSink() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 1);

        createMapping("map", Integer.class, Integer.class);

        String sql = "EXPLAIN PLAN FOR SINK INTO map(__key, this) VALUES (2, 2)";
        assertRowsAnyOrder(sql, singletonList(
                new Row("SinkMapPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]], values=" +
                        "[[{expressions=[[" +
                        "ConstantExpression{type=QueryDataType {family=INTEGER}, value=2}, " +
                        "ConstantExpression{type=QueryDataType {family=INTEGER}, value=2}]]}]])"
                )));
    }

    @Test
    public void test_explainStatementUpdate() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 1);
        map.put(2, 10);

        createMapping("map", Integer.class, Integer.class);

        // (Optimized) Update by single key
        String sql = "EXPLAIN PLAN FOR UPDATE map SET this = 2 WHERE __key = 1";
        assertRowsAnyOrder(sql, singletonList(
                new Row("UpdateByKeyMapPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1], " +
                        "filter==($0, 1)]]], keyCondition=[1], updatedColumns=[[this]], sourceExpressions=[[2]])")
        ));

        // Update by multiple keys
        sql = "EXPLAIN PLAN FOR UPDATE map SET this = 2 WHERE __key = 1 AND __key = 2";
        assertRowsAnyOrder(sql, asList(
                new Row("UpdatePhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]], operation=[UPDATE], " +
                        "updateColumnList=[[this]], sourceExpressionList=[[2]], flattened=[false])"),
                new Row("  ValuesPhysicalRel(values=[{expressions=[]}])")
        ));
    }

    @Test
    public void test_explainStatementDelete() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        createMapping("map", Integer.class, Integer.class);

        // (Optimized) Delete by single key
        String sql = "EXPLAIN PLAN FOR DELETE FROM map WHERE __key = 1";
        assertRowsAnyOrder(sql, singletonList(
                new Row("DeleteByKeyMapPhysicalRel(table=[[hazelcast, public, map[projects=[0], " +
                        "filter==($0, 1)]]], keyCondition=[1])")
        ));

        // Common Delete by single key
        sql = "EXPLAIN PLAN FOR DELETE FROM map";
        assertRowsAnyOrder(sql, asList(
                new Row("DeletePhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]], " +
                        "operation=[DELETE], flattened=[false])"),
                new Row("  FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[0]]]])")

        ));
    }
}
