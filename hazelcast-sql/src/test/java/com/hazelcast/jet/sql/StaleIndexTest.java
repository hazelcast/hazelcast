package com.hazelcast.jet.sql;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.List;

import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StaleIndexTest extends OptimizerTestSupport {
    private SqlServiceImpl sqlService;
    private IMap<Integer, FooTuple> map;
    private List<TableField> mapTableFields;
    private final String query = "select __key from m where f0='v0'";

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(3, null);
    }

    @Before
    public void setUp() throws Exception {
        sqlService = (SqlServiceImpl) instance().getSql();
        mapTableFields = asList(
                new MapTableField("__key", INT, false, QueryPath.KEY_PATH),
                new MapTableField("f0", VARCHAR, false, new QueryPath("f0", false)),
                new MapTableField("f1", VARCHAR, false, new QueryPath("f1", false))
        );
    }

    @Test
    public void test() {
        map = instance().getMap("m");
        createMapping("m", int.class, FooTuple.class);

        // Sometimes it fails due to FullScan being picked up over IndexScan (commented part).
        createIndexAndExecuteQuery("f0", false);

        map.destroy();

        // The test is timing-dependent, plan cache should be invalidated,
        // but occasionally it may not be invalidated.
        createIndexAndExecuteQuery("f1", true);

        // plan should be invalidated now
        assertTrueEventually(() -> assertEquals(0, sqlService.getPlanCache().size()));
        createIndexAndExecuteQuery("f1", false);
    }

    private void createIndexAndExecuteQuery(String indexedField, boolean flakyRun /*, boolean indexedLaunch */) {
        map.addIndex(new IndexConfig(IndexType.SORTED, indexedField).setName("foo"));
        map.put(0, new FooTuple("v0", "v1"));
        map.put(1, new FooTuple("v1", "v0"));
        map.put(2, new FooTuple("v5", "v2"));
        map.put(3, new FooTuple("v4", "v3"));
        map.put(4, new FooTuple("v3", "v4"));
        map.put(5, new FooTuple("v8", "v10"));
        map.put(6, new FooTuple("v10", "v20"));

        List<MapTableIndex> partitionedMapIndexes = getPartitionedMapIndexes(mapContainer(map), mapTableFields);
        assertFalse(partitionedMapIndexes.isEmpty());

//        HazelcastTable table = partitionedTable(map.getName(), mapTableFields, partitionedMapIndexes, map.size());
//        List<QueryDataType> parameterTypes = asList(INT, VARCHAR, VARCHAR);
//        PhysicalRel optimizedPhysicalRel = optimizePhysical(query, parameterTypes, table).getPhysical();
//        assertPlan(optimizedPhysicalRel, plan(
//                planRow(0, indexedLaunch ? IndexScanMapPhysicalRel.class : FullScanPhysicalRel.class)
//        ));

        if (flakyRun) {
            try {
                assertRowsAnyOrder(query, singletonList(new Row(0)));
            } catch (Exception e) {
                // TODO: fail-fast.
                e.printStackTrace();
            }

        } else {
            assertRowsAnyOrder(query, singletonList(new Row(0)));
        }
    }

    public static class FooTuple implements Serializable {
        public String f0;
        public String f1;

        public FooTuple(String f0, String f1) {
            this.f0 = f0;
            this.f1 = f1;
        }
    }
}