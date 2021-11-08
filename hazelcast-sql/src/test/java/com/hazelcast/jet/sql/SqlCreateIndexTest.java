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
        for (int i = 0; i < 200; ++i) {
            map.put(i, i);
        }
    }

    @Test
    public void basicSqlCreateIndexTest_hash() {
        String indexName = SqlTestSupport.randomName();
        String selectSql = "SELECT * FROM " + MAP_NAME + " WHERE this = 100";

        checkPlan(false, false, selectSql);

        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this) TYPE HASH";
        instance().getSql().execute(sql);

        checkPlan(true, false, selectSql);
    }

    @Test
    public void basicSqlCreateIndexTest_sorted() {
        String indexName = SqlTestSupport.randomName();
        String selectSql = "SELECT * FROM " + MAP_NAME + " ORDER BY this DESC";

        checkPlan(false, true, selectSql);

        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this) TYPE SORTED";
        instance().getSql().execute(sql);

        checkPlan(true, true, selectSql);
    }

    @Test
    public void basicSqlCreateIndexTest_bitmap() {
        assertThat(mapContainer(map).getIndexes().getIndex(MAP_NAME)).isNull();

        String indexName = SqlTestSupport.randomName();
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (__key) TYPE BITMAP " +
                "OPTIONS ('unique_key' = '__key' , 'unique_key_transformation' = 'OBJECT'); ";

        instance().getSql().execute(sql);

        assertThat(mapContainer(map).getIndexes().getIndex(indexName)).isNotNull();
    }

    @Test
    public void sqlCreateIndexWithDefaultTypeTest() {
        String indexName = SqlTestSupport.randomName();
        String selectSql = "SELECT * FROM " + MAP_NAME + " ORDER BY this DESC";

        checkPlan(false, true, selectSql);

        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this)";
        instance().getSql().execute(sql);

        checkPlan(true, true, selectSql);
    }

    @Test
    public void indexArgsCannotBeDuplicatedTest() {
        String sql = "CREATE INDEX IF NOT EXISTS idx ON " + MAP_NAME + " (this, this) TYPE BITMAP";
        assertThatThrownBy(() -> instance().getSql().execute(sql))
                .hasMessageContaining("specified more than once");
    }

    @Test
    public void indexOptionsCannotBeDuplicatedTest() {
        String indexName = SqlTestSupport.randomName();
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (__key) TYPE BITMAP " +
                "OPTIONS ('unique_key' = '__key' , 'unique_key' = 'OBJECT'); ";

        assertThatThrownBy(() -> instance().getSql().execute(sql))
                .hasMessageContaining("Option 'unique_key' specified more than once");
    }

    @Test
    public void indexExistsTest() {
        createMapping(MAP_NAME, Integer.class, Integer.class);

        String indexName = "idx";
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this) TYPE HASH";
        instance().getSql().execute(sql);

        String sql2 = "CREATE INDEX " + indexName + " ON " + MAP_NAME + " (this) TYPE HASH";
        assertThatThrownBy(() -> instance().getSql().execute(sql2))
                .hasMessageContaining("Can't create index: index 'idx' already exists");
    }

    @Test
    public void indexExistsWithExistingIndexTest() {
        createMapping(MAP_NAME, Integer.class, Integer.class);

        String indexName = "idx";
        map.addIndex(new IndexConfig(IndexType.HASH, "this").setName(indexName));

        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this) TYPE HASH";

        assertThat(instance().getSql().execute(sql)).isNotNull();
    }

    @Test
    public void indexOptionsAbsentTest() {
        String sql1 = "CREATE INDEX IF NOT EXISTS idx ON " + MAP_NAME + " (__key) TYPE BITMAP " +
                "OPTIONS ('unique_key' = '__key')";

        assertThatThrownBy(() -> instance().getSql().execute(sql1))
                .hasMessageContaining("Required option missing: unique_key_transformation");

        String sql2 = "CREATE INDEX IF NOT EXISTS idx ON " + MAP_NAME + " (__key) TYPE BITMAP " +
                "OPTIONS ('unique_key_transformation' = 'RAW')";

        assertThatThrownBy(() -> instance().getSql().execute(sql2))
                .hasMessageContaining("Required option missing: unique_key");
    }

    @Test
    public void hashAndSortedIndicesMustNotContainOptionsTest() {
        String sql1 = "CREATE INDEX IF NOT EXISTS idx ON " + MAP_NAME + " (this) TYPE HASH OPTIONS ('a' = '1')";
        assertThatThrownBy(() -> instance().getSql().execute(sql1))
                .hasMessageContaining("Unknown option for HASH index: a");

        String sql2 = "CREATE INDEX IF NOT EXISTS idx2 ON " + MAP_NAME + " (this) TYPE SORTED OPTIONS ('a' = '1')";
        assertThatThrownBy(() -> instance().getSql().execute(sql2))
                .hasMessageContaining("Unknown option for SORTED index: a");
    }

    @Test
    public void bitmapIndexDoesNotContainOptionsTest() {
        String sql = "CREATE INDEX IF NOT EXISTS idx ON " + MAP_NAME + " (this) TYPE BITMAP";
        assertThatThrownBy(() -> instance().getSql().execute(sql))
                .hasMessageContaining("BITMAP index requires unique_key and unique_key_transformation options");
    }

    private void checkPlan(boolean withIndex, boolean sorted, String sql) {
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.INT);
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("this", QueryDataType.INT, false, QueryPath.VALUE_PATH)
        );
        HazelcastTable table = partitionedTable(
                MAP_NAME,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(map), mapTableFields),
                map.size()
        );
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
