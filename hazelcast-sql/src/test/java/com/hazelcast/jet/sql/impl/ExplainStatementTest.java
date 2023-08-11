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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Arrays;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

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
        assertRowsOrdered(sql, singletonList(new Row(
                "FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], discriminator=[0])")
        ));

        sql = "EXPLAIN PLAN FOR SELECT * FROM map";
        assertRowsOrdered(sql, singletonList(new Row(
                "FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], discriminator=[0])")
        ));
    }

    @Test
    public void test_explainStatementDynamicParams() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 10);

        createMapping("map", Integer.class, Integer.class);

        String sql = "EXPLAIN SELECT * FROM map WHERE __key = ?";
        SqlStatement statement = new SqlStatement(sql).addParameter(1);
        SqlResult result = instance().getSql().execute(statement);

        String expectedScanRes = "SelectByKeyMapPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1], " +
                "filter==($0, ?0)]]], keyCondition=[?0], projections=[__key=[$0], this=[$1]])";
        assertEquals(expectedScanRes, result.iterator().next().getObject(0));
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
        assertRowsOrdered(sql, singletonList(
                new Row("IndexScanMapPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], " +
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
        assertRowsOrdered(sql, singletonList(
                new Row("IndexScanMapPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], " +
                        "index=[map_sorted_this], indexExp=[null], remainderExp=[null])")
        ));
    }

    @Test
    public void test_explainStatementLimitOffsetWithIndexScan() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        map.put(4, 4);
        map.addIndex(IndexType.SORTED, "this");

        String sql = "EXPLAIN PLAN FOR SELECT * FROM map ORDER BY this LIMIT 1 OFFSET 1";

        createMapping("map", Integer.class, Integer.class);
        assertRowsOrdered(sql, asList(
                new Row("LimitPhysicalRel(offset=[1:TINYINT(1)], fetch=[1:TINYINT(1)])"),
                new Row("  IndexScanMapPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], " +
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
        assertRowsOrdered(sql, asList(
                new Row("SortPhysicalRel(sort0=[$1], dir0=[DESC])"),
                new Row("  UnionPhysicalRel(all=[true])"),
                new Row("    FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], discriminator=[0])"),
                new Row("    FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], discriminator=[0])")
        ));
    }

    @Test
    public void test_explainStatementSelectBelowUnion() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 10);

        String sql = "EXPLAIN PLAN FOR SELECT * FROM map UNION ALL SELECT * FROM map";

        createMapping("map", Integer.class, Integer.class);
        assertRowsOrdered(sql, asList(
                new Row("UnionPhysicalRel(all=[true])"),
                new Row("  FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], discriminator=[0])"),
                new Row("  FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], discriminator=[0])")
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

        assertRowsOrdered(sql, asList(
                new Row("CalcPhysicalRel(expr#0..5=[{inputs}], __key=[$t0], name=[$t4])"),
                new Row("  JoinNestedLoopPhysicalRel(condition=[=($0, $3)], joinType=[inner], conditionType=[equiJoin])"),
                new Row("    FullScanPhysicalRel(table=[[hazelcast, public, map1[projects=[$0, $1]]]], discriminator=[0])"),
                new Row("    FullScanPhysicalRel(table=[[hazelcast, public, map2[projects=[$0, $1, $2, $3]]]], discriminator=[0])")
        ));
    }

    @Test
    public void test_explainStatementInsert() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 1);

        String sql = "EXPLAIN PLAN FOR INSERT INTO map VALUES (2, 2)";

        createMapping("map", Integer.class, Integer.class);

        assertRowsOrdered(sql, singletonList(
                new Row("InsertMapPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], values=[{" +
                        "expressions=[[" +
                        "ConstantExpression{type=QueryDataType {family=INTEGER}, value=2}, " +
                        "ConstantExpression{type=QueryDataType {family=INTEGER}, value=2}]]}])"
                )));

        createMapping("map", Integer.class, Integer.class);

        sql = "EXPLAIN PLAN FOR INSERT INTO map VALUES (3, 3), (4, 4)";
        assertRowsOrdered(sql, asList(
                new Row("InsertPhysicalRel(" +
                        "table=[[hazelcast, public, map[projects=[$0, $1]]]], operation=[INSERT], flattened=[false])"),
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
        assertRowsOrdered(sql, singletonList(
                new Row("SinkMapPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], values=" +
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
        assertRowsOrdered(sql, singletonList(
                new Row("UpdateByKeyMapPhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1, 2], " +
                        "filter==($0, 1)]]], keyCondition=[1], updatedColumns=[[this]], sourceExpressions=[[2]])")
        ));

        // Update by multiple keys
        sql = "EXPLAIN PLAN FOR UPDATE map SET this = 2 WHERE __key = 1 AND __key = 2";
        assertRowsOrdered(sql, asList(
                new Row("UpdatePhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], " +
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
        assertRowsOrdered(sql, singletonList(
                new Row("DeleteByKeyMapPhysicalRel(table=[[hazelcast, public, map[projects=[$0], " +
                        "filter==($0, 1)]]], keyCondition=[1])")
        ));

        // Common Delete by single key
        sql = "EXPLAIN PLAN FOR DELETE FROM map";
        assertRowsOrdered(sql, asList(
                new Row("DeletePhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], " +
                        "flattened=[false])"),
                new Row("  FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[$0]]]], discriminator=[0])")
        ));

        // Delete by predicate
        sql = "EXPLAIN PLAN FOR DELETE FROM map WHERE __key > 1";
        assertRowsOrdered(sql, asList(
                new Row("DeletePhysicalRel(table=[[hazelcast, public, map[projects=[$0, $1]]]], " +
                        "flattened=[false])"),
                new Row("  FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[$0], " +
                        "filter=>($0, 1)]]], discriminator=[0])")
        ));
    }

    @Test
    public void test_explainStatementShowShouldThrowParserEx() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 10);

        String sql = "EXPLAIN SHOW MAPPINGS";

        assertThatThrownBy(() -> instance().getSql().execute(sql))
                .hasMessageContaining("Incorrect syntax near the keyword 'SHOW'");
    }

    @Test
    public void test_partitioningKeyInfoWithSimpleKey() {
        createMapping("test", Long.class, String.class);
        // non-prunable
        assertRowsOrdered("EXPLAIN PLAN FOR SELECT this FROM test WHERE this = '1'", rows(1,
                "FullScanPhysicalRel(table=[[hazelcast, public, test[projects=[$1], filter==($1, _UTF-16LE'1')]]], discriminator=[0])"
        ));
        // prunable
        assertRowsOrdered("EXPLAIN PLAN FOR SELECT this FROM test WHERE __key = 1 AND this = '1'", rows(1,
                "FullScanPhysicalRel(table=[[hazelcast, public, test[projects=[$1], filter=AND(=($0, 1), =($1, _UTF-16LE'1'))]]], discriminator=[0], partitioningKey=[$0], partitioningKeyValues=[(1:BIGINT(63))])"
        ));
    }

    @Test
    public void test_partitioningKeyInfoWithComplexKey() {
        instance().getConfig().addMapConfig(new MapConfig("testMap").setPartitioningAttributeConfigs(Arrays.asList(
                new PartitioningAttributeConfig("comp1"),
                new PartitioningAttributeConfig("comp2")
        )));

        instance().getSql().execute("CREATE MAPPING test EXTERNAL NAME \"testMap\" ("
                + "c1 BIGINT EXTERNAL NAME \"__key.comp3\","
                + "c2 BIGINT EXTERNAL NAME \"__key.comp2\","
                + "c3 BIGINT EXTERNAL NAME \"__key.comp1\","
                + "this VARCHAR"
                + ") TYPE IMap OPTIONS ("
                + "'valueFormat'='varchar', "
                + "'keyFormat'='java', "
                + "'keyJavaClass'='" + KeyObj.class.getName() + "')");

        // non-prunable
        assertRowsOrdered("EXPLAIN PLAN FOR SELECT this FROM test WHERE c1 = ? AND c2 = ?", rows(1,
                "FullScanPhysicalRel(table=[[hazelcast, public, "
                        + "test[projects=[$4], "
                        + "filter=AND(=($0, ?0), =($1, ?1))]]], "
                        + "discriminator=[0])"
        ));
        // prunable
        assertRowsOrdered("EXPLAIN PLAN FOR SELECT this FROM test WHERE c3 = 1 AND c2 = ?", rows(1,
                "FullScanPhysicalRel(table=[[hazelcast, public, "
                        + "test[projects=[$4], "
                        + "filter=AND(=($2, 1), =($1, ?0))]]], "
                        + "discriminator=[0], "
                        + "partitioningKey=[$1, $2], "
                        + "partitioningKeyValues=[(?0, 1:BIGINT(63))])"
        ));
    }

    @Test
    public void test_scanPruningWithoutMemberPruningSimpleQuery() {
        instance().getConfig().addMapConfig(new MapConfig("testMap").setPartitioningAttributeConfigs(Arrays.asList(
                new PartitioningAttributeConfig("comp1"),
                new PartitioningAttributeConfig("comp2")
        )));

        instance().getSql().execute("CREATE MAPPING test EXTERNAL NAME \"testMap\" ("
                + "c1 BIGINT EXTERNAL NAME \"__key.comp3\","
                + "c2 BIGINT EXTERNAL NAME \"__key.comp2\","
                + "c3 BIGINT EXTERNAL NAME \"__key.comp1\","
                + "this VARCHAR"
                + ") TYPE IMap OPTIONS ("
                + "'valueFormat'='varchar', "
                + "'keyFormat'='java', "
                + "'keyJavaClass'='" + KeyObj.class.getName() + "')");

        // simple query for which member pruning is not possible
        assertRowsAnyOrder("EXPLAIN PLAN FOR SELECT this FROM test WHERE c3 = 1 AND c2 = ?" +
                " UNION ALL SELECT this FROM test", rows(1,
                // order of children is not important
                "UnionPhysicalRel(all=[true])",
                // pruned scan
                "  FullScanPhysicalRel(table=[[hazelcast, public, "
                        + "test[projects=[$4], "
                        + "filter=AND(=($2, 1), =($1, ?0))]]], "
                        + "discriminator=[0], "
                        + "partitioningKey=[$1, $2], "
                        + "partitioningKeyValues=[(?0, 1:BIGINT(63))])",
                // not pruned scan
                "  FullScanPhysicalRel(table=[[hazelcast, public, test[projects=[$4]]]], discriminator=[0])"
        ));
    }

    @Test
    public void test_scanPruningWithoutMemberPruning() {
        instance().getConfig().addMapConfig(new MapConfig("testMap").setPartitioningAttributeConfigs(Arrays.asList(
                new PartitioningAttributeConfig("comp1"),
                new PartitioningAttributeConfig("comp2")
        )));

        instance().getSql().execute("CREATE MAPPING test EXTERNAL NAME \"testMap\" ("
                + "c1 BIGINT EXTERNAL NAME \"__key.comp1\","
                + "c2 BIGINT EXTERNAL NAME \"__key.comp2\","
                + "c3 BIGINT EXTERNAL NAME \"__key.comp3\","
                + "this VARCHAR"
                + ") TYPE IMap OPTIONS ("
                + "'valueFormat'='varchar', "
                + "'keyFormat'='java', "
                + "'keyJavaClass'='" + KeyObj.class.getName() + "')");

        // complicated query that should not be eligible for member pruning
        // but at least one side of the join should execute full scan that is eligible for scan partition pruning
        // - it does not matter if nested loops or hash join is used.
        var plan = allRows("EXPLAIN SELECT max(b.this), b.c2 " +
                "FROM test a join test b on a.c1 = b.c2 " +
                "WHERE a.c1 = 1 AND a.c2 = ? and b.c1 = 1 AND b.c2 = ? " +
                "GROUP BY b.c2 ORDER BY b.c2", instance().getSql())
                .stream().map(row -> ((String) row.getValues()[0]).trim())
                .collect(Collectors.toUnmodifiableList());
        assertThat(plan).as("At least one of the IMap scans should be pruned")
                .anySatisfy(row ->
                        assertThat(row)
                                .startsWith("FullScanPhysicalRel(table=[[hazelcast, public, test[")
                                .contains("partitioningKey=[$0, $1]")
                                .contains("partitioningKeyValues=["));
    }

    public static class KeyObj implements Serializable {
        public Long comp1;
        public Long comp2;
        public Long comp3;

        public KeyObj() {

        }

        public KeyObj(final Long comp1, final Long comp2, final Long comp3) {
            this.comp1 = comp1;
            this.comp2 = comp2;
            this.comp3 = comp3;
        }
    }
}
