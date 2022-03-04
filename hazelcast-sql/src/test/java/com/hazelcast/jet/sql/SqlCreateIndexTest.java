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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SortPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlCreateIndexTest extends OptimizerTestSupport {

    private static final String MAP_NAME = "map";
    private IMap<Integer, Integer> map;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void setUp() throws Exception {
        map = instance().getMap(MAP_NAME);
        for (int i = 0; i < 500; ++i) {
            map.put(i, i);
        }
    }

    @Test
    public void when_basicHashIndexCreated_then_succeeds() {
        String indexName = SqlTestSupport.randomName();
        String selectSql = "SELECT * FROM " + MAP_NAME + " WHERE this = 100";

        checkPlan(false, false, selectSql);

        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this) TYPE HASH";
        instance().getSql().execute(sql);

        checkPlan(true, false, selectSql);
    }

    @Test
    public void when_basicSortedIndexCreated_then_succeeds() {
        String indexName = SqlTestSupport.randomName();
        String selectSql = "SELECT * FROM " + MAP_NAME + " ORDER BY this DESC";

        checkPlan(false, true, selectSql);

        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this) TYPE SORTED";
        instance().getSql().execute(sql);

        checkPlan(true, true, selectSql);
    }

    @Test
    public void when_basicBitmapIndexCreated_then_succeeds() {
        assertThat(mapContainer(map).getIndexes().getIndex(MAP_NAME)).isNull();

        String indexName = SqlTestSupport.randomName();
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (__key) TYPE BITMAP; ";

        instance().getSql().execute(sql);

        assertThat(mapContainer(map).getIndexes().getIndex(indexName)).isNotNull();
    }

    @Test
    public void when_bitmapIndexWithOptionCreated_then_succeeds() {
        assertThat(mapContainer(map).getIndexes().getIndex(MAP_NAME)).isNull();

        String indexName = SqlTestSupport.randomName();
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (__key) TYPE BITMAP " +
                "OPTIONS ('unique_key' = '__key' , 'unique_key_transformation' = 'OBJECT'); ";

        instance().getSql().execute(sql);

        assertThat(mapContainer(map).getIndexes().getIndex(indexName)).isNotNull();
    }

    @Test
    public void when_indexCreatedWithEmptyMap_then_succeeds() {
        String mapName = "map2";
        String indexName = SqlTestSupport.randomName();
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + mapName + " (__key) TYPE SORTED";

        instance().getSql().execute(sql);

        assertThat(mapContainer(instance().getMap(mapName)).getIndexes().getIndex(indexName)).isNotNull();
    }

    @Test
    public void when_indexCreatedWithDefaultType_then_succeeds() {
        String indexName = SqlTestSupport.randomName();
        String selectSql = "SELECT * FROM " + MAP_NAME + " ORDER BY this DESC";

        checkPlan(false, true, selectSql);

        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this)";
        instance().getSql().execute(sql);

        checkPlan(true, true, selectSql);
    }

    @Test
    public void when_indexCreatedWithDuplicatedArguments_then_throws() {
        String sql = "CREATE INDEX IF NOT EXISTS idx ON " + MAP_NAME + " (this, this) TYPE BITMAP";
        assertThatThrownBy(() -> instance().getSql().execute(sql))
                .hasMessageContaining("specified more than once");
    }

    @Test
    public void when_indexCreatedWithDuplicatedOptions_then_throws() {
        String indexName = SqlTestSupport.randomName();
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (__key) TYPE BITMAP " +
                "OPTIONS ('unique_key' = '__key' , 'unique_key' = 'OBJECT'); ";

        assertThatThrownBy(() -> instance().getSql().execute(sql))
                .hasMessageContaining("Option 'unique_key' specified more than once");
    }

    @Test
    public void when_indexCreatedWithIfNotExistsAndCreatedAgain_then_throws() {
        createMapping(MAP_NAME, Integer.class, Integer.class);

        String indexName = "idx";
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this) TYPE HASH";
        instance().getSql().execute(sql);

        String sql2 = "CREATE INDEX " + indexName + " ON " + MAP_NAME + " (this) TYPE HASH";
        assertThatThrownBy(() -> instance().getSql().execute(sql2))
                .hasMessageContaining("Can't create index: index 'idx' already exists");
    }

    @Test
    public void when_indexExistsAndCreatedAgainWithIfNotExists_then_succeeds() {
        createMapping(MAP_NAME, Integer.class, Integer.class);

        String indexName = "idx";
        map.addIndex(new IndexConfig(IndexType.HASH, "this").setName(indexName));

        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this) TYPE HASH";

        assertThat(instance().getSql().execute(sql)).isNotNull();
    }

    @Test
    public void when_hashOrSortedIndexCreatedWithOptions_then_throws() {
        String sql1 = "CREATE INDEX IF NOT EXISTS idx ON " + MAP_NAME + " (this) TYPE HASH OPTIONS ('a' = '1')";
        assertThatThrownBy(() -> instance().getSql().execute(sql1))
                .hasMessageContaining("Unknown option for HASH index: a");

        String sql2 = "CREATE INDEX IF NOT EXISTS idx2 ON " + MAP_NAME + " (this) TYPE SORTED OPTIONS ('a' = '1')";
        assertThatThrownBy(() -> instance().getSql().execute(sql2))
                .hasMessageContaining("Unknown option for SORTED index: a");
    }

    private void checkPlan(boolean withIndex, boolean sorted, String sql) {
        checkPlan(withIndex, sorted, sql, MAP_NAME);
    }

    private void checkPlan(boolean withIndex, boolean sorted, String sql, String mapName) {
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.INT);
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("this", QueryDataType.INT, false, QueryPath.VALUE_PATH)
        );
        HazelcastTable table = partitionedTable(
                mapName,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(map), mapTableFields),
                map.size()
        );

        assertThat(table.getProjects().size()).isEqualTo(2);
        OptimizerTestSupport.Result optimizationResult = optimizePhysical(sql, parameterTypes, table);
        if (sorted) {
            if (withIndex) {
                assertPlan(
                        optimizationResult.getPhysical(),
                        plan(planRow(0, IndexScanMapPhysicalRel.class))
                );
            } else {
                assertPlan(
                        optimizationResult.getPhysical(),
                        plan(
                                planRow(0, SortPhysicalRel.class),
                                planRow(1, FullScanPhysicalRel.class)
                        )
                );
            }
        } else {
            assertPlan(
                    optimizationResult.getLogical(),
                    plan(
                            planRow(0, FullScanLogicalRel.class)
                    )
            );
            assertPlan(
                    optimizationResult.getPhysical(),
                    plan(planRow(0, withIndex ? IndexScanMapPhysicalRel.class : FullScanPhysicalRel.class))
            );
        }
    }
}
