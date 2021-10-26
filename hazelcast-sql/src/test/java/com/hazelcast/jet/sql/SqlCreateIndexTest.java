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
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.SortLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SortPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
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

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
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
        createMapping(MAP_NAME, Integer.class, Integer.class);

        String indexName = SqlTestSupport.randomName();
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this) TYPE HASH";
        String selectSql = "SELECT * FROM " + MAP_NAME + " WHERE this = 100";

        checkPlan(false, false, selectSql);

        instance().getSql().execute(sql);

        checkPlan(true, false, selectSql);
    }

    @Test
    public void basicSqlCreateIndexTest_sorted() {
        createMapping(MAP_NAME, Integer.class, Integer.class);

        String indexName = SqlTestSupport.randomName();
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this) TYPE SORTED";
        String selectSql = "SELECT * FROM " + MAP_NAME + " ORDER BY this DESC";

        checkPlan(false, true, selectSql);

        instance().getSql().execute(sql);

        checkPlan(true, true, selectSql);
    }

    @Test
    public void basicSqlCreateIndexTest_bitmap() {
        createMapping(MAP_NAME, Integer.class, Integer.class);

        assertThat(mapContainer(map).getIndexes().getIndex(MAP_NAME)).isNull();

        String indexName = SqlTestSupport.randomName();
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (__key) TYPE BITMAP " +
                "OPTIONS ('unique_key' = '__key' , 'unique_key_transformation' = 'OBJECT'); ";

        instance().getSql().execute(sql);

        assertThat(mapContainer(map).getIndexes().getIndex(indexName)).isNotNull();
    }

    @Test
    public void sqlCreateIndexWithDefaultTypeTest() {
        createMapping(MAP_NAME, Integer.class, Integer.class);

        String indexName = SqlTestSupport.randomName();
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this)";
        String selectSql = "SELECT * FROM " + MAP_NAME + " ORDER BY this DESC";

        checkPlan(false, true, selectSql);

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
    public void unsupportedMappingTypeForIndexCreationTest() {
        String indexName = "idx";
        try (SqlResult result = instance().getSql().execute("CREATE OR REPLACE MAPPING " + MAP_NAME
                + " EXTERNAL NAME " + MAP_NAME + "\n"
                + " TYPE " + KafkaSqlConnector.TYPE_NAME + "\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + "'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_VALUE_CLASS + "'='" + Integer.class.getName() + "'\n"
                + ")"
        )) {
            assertThat(result.updateCount()).isEqualTo(0);
            String sql = "CREATE INDEX " + indexName + " ON " + MAP_NAME + " (this) TYPE HASH";
            assertThatThrownBy(() -> instance().getSql().execute(sql))
                    .hasMessageContaining("Can't create index: only IMap supports index creation");
        }
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
                .hasMessageContaining("Only BITMAP index requires an options");

        String sql2 = "CREATE INDEX IF NOT EXISTS idxx ON " + MAP_NAME + " (this) TYPE SORTED OPTIONS ('a' = '1')";
        assertThatThrownBy(() -> instance().getSql().execute(sql2))
                .hasMessageContaining("Only BITMAP index requires an options");
    }

    @Test
    public void bitmapIndexDoesntContainOptionsTest() {
        String sql = "CREATE INDEX IF NOT EXISTS idx ON " + MAP_NAME + " (this) TYPE BITMAP";
        assertThatThrownBy(() -> instance().getSql().execute(sql))
                .hasMessageContaining("Cant create BITMAP index: bitmap index config is empty");
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
            plan(
                    planRow(0, SortLogicalRel.class),
                    planRow(1, FullScanLogicalRel.class)
            );
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
