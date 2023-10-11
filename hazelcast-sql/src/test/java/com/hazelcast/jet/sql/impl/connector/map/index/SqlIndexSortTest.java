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

package com.hazelcast.jet.sql.impl.connector.map.index;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.sql.impl.SqlEndToEndTestSupport;
import com.hazelcast.map.IMap;
import org.assertj.core.api.Condition;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

/**
 * Make sure that index scan result is sorted only when needed
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SqlIndexSortTest extends SqlEndToEndTestSupport {

    private static final String MAP_NAME = "map";
    private static final String INDEX_SCAN_VERTEX_NAME = "Index(IMap[public." + MAP_NAME + "])";

    @BeforeClass
    public static void beforeClass() {
        initialize(1, smallInstanceConfig());
    }

    @Before
    public void before() {
        IndexConfig indexConfig = new IndexConfig()
                .setName("index")
                .setType(IndexType.SORTED)
                .addAttribute("this");

        instance().getMap(MAP_NAME).addIndex(indexConfig);

        IMap map = instance().getMap(MAP_NAME);
        for (int i = 0; i < 1000; ++i) {
            map.put(i, i * 10);
        }

        createMapping(MAP_NAME, Integer.class, Integer.class);
    }

    @Test
    public void shouldUseSortCombine_whenOrderByNoAggregation() {
        instance().getSql().execute("select * from " + MAP_NAME + " order by this").close();
        assertThatDag().has(indexScan).has(sortCombine).doesNotHave(sort);
    }

    @Test
    public void shouldSortLate_whenOrderByWithJoin() {
        instance().getSql().execute("select m1.this from " + MAP_NAME + " m1 " +
                "join " + MAP_NAME + " m2 on m2.__key = m1.this " +
                "where m1.this < 5 order by m1.this + m2.this").close();
        // convoluted order by is to ensure that index cannot be simply used to get requested order
        assertThatDag().has(indexScan).has(sortCombine).has(sort)
                .has(edgeBetween("Sort", "SortCombine"))
                .doesNotHave(edgeBetween(INDEX_SCAN_VERTEX_NAME, "SortCombine"))
                .doesNotHave(edgeBetween(INDEX_SCAN_VERTEX_NAME, "Sort"));
    }

    @Test
    public void shouldNotUseSortCombine_whenAggregationWithoutOrderBy() {
        instance().getSql().execute("select sum(this) from " + MAP_NAME + " where this between 5 and 15").close();
        assertThatDag().has(indexScan).doesNotHave(sortCombine).doesNotHave(sort);
    }

    @Test
    public void shouldNotUseSortCombine_whenUnion() {
        instance().getSql().execute("select this from " + MAP_NAME + " where this < 5"
             + "union select this from " + MAP_NAME + " where this > 5").close();
        assertThatDag().has(indexScan).doesNotHave(sortCombine).doesNotHave(sort);
    }

    @Test
    public void shouldNotUseSortCombine_whenUnionAll() {
        instance().getSql().execute("select this from " + MAP_NAME + " where this < 5" +
                "union all select this from " + MAP_NAME + " where this > 5").close();
        assertThatDag().has(indexScan).doesNotHave(sortCombine).doesNotHave(sort);
    }

    private Condition<DAG> indexScan = new Condition<>(
            dag -> dag.vertices().stream().anyMatch(v -> v.getName().equals(INDEX_SCAN_VERTEX_NAME)),
            "Should use index scan");
    private Condition<DAG> sortCombine = new Condition<>(
            dag -> dag.vertices().stream().anyMatch(v -> v.getName().equals("SortCombine")),
            "Should combine index scan output");
    private Condition<DAG> sort = new Condition<>(
            dag -> dag.vertices().stream().anyMatch(v -> v.getName().equals("Sort")),
            "Should sort the result output");

    private Condition<DAG> edgeBetween(String sourceName, String destName) {
        return new Condition<>((DAG dag) ->
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(dag.edgeIterator(), Spliterator.ORDERED), false)
                    .anyMatch(e -> e.getSourceName().equals(sourceName) && e.getDestName().equals(destName)),
                "Should have edge between " + sourceName + " and " + destName);
    }
}
