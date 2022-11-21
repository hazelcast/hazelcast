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
import org.junit.Ignore;
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

    // Test is flaky, because cache invalidation is running once per second, and it isn't always possible to reach it.
    @Ignore
    @Test
    public void test() {
        map = instance().getMap("m");
        createMapping("m", int.class, FooTuple.class);

        // Sometimes it fails due to FullScan being picked up over IndexScan (commented part).
        createIndexAndExecuteQuery("f0");

        map.destroy();

        // The test is timing-dependent, plan cache should be invalidated,
        // but occasionally it may not be invalidated.
        createIndexAndExecuteQuery("f1");

        // plan should be invalidated now
        assertTrueEventually(() -> assertEquals(0, sqlService.getPlanCache().size()));
        createIndexAndExecuteQuery("f1");
    }

    private void createIndexAndExecuteQuery(String indexedField  /*, boolean indexedLaunch */) {
        map.addIndex(new IndexConfig(IndexType.SORTED, indexedField).setName("foo"));
        map.put(0, new FooTuple("v0", "v1"));
        map.put(1, new FooTuple("v1", "v0"));
        map.put(2, new FooTuple("v5", "v2"));
        map.put(3, new FooTuple("v4", "v3"));
        map.put(4, new FooTuple("v3", "v4"));
        map.put(5, new FooTuple("v8", "v10"));
        map.put(6, new FooTuple("v10", "v20"));

        List<MapTableIndex> partitionedMapIndexes = getPartitionedMapIndexes(mapContainer(map), mapTableFields);
        assertEquals(1, partitionedMapIndexes.size());

        assertRowsAnyOrder(query, singletonList(new Row(0)));
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
