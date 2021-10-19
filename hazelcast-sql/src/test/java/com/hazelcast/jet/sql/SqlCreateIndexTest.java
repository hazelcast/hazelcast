package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.SortLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SortPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
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

@RunWith(HazelcastParallelClassRunner.class)
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
        createMapping(MAP_NAME, Integer.class, Integer.class);
    }

    @Test
    public void basicSqlCreateIndexTest_hash() {
        String indexName = SqlTestSupport.randomName();
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this) TYPE HASH";
        String selectSql = "SELECT * FROM " + MAP_NAME + " WHERE this = 100";

        checkPlan(false, false, selectSql);

        instance().getSql().execute(sql);

        checkPlan(true, false, selectSql);
    }

    @Test
    public void basicSqlCreateIndexTest_sorted() {
        String indexName = SqlTestSupport.randomName();
        String sql = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + MAP_NAME + " (this) TYPE SORTED";
        String selectSql = "SELECT * FROM " + MAP_NAME + " ORDER BY this DESC";

        checkPlan(false, true, selectSql);

        instance().getSql().execute(sql);

        checkPlan(true, true, selectSql);
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
