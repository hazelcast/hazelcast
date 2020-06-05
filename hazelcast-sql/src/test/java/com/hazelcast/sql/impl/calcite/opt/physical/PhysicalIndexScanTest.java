/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.config.IndexType;
import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import org.apache.calcite.schema.Table;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;

/**
 * Tests for physical index path selection.
 */
public class PhysicalIndexScanTest extends OptimizerTestSupport {
    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = OptimizerTestSupport.partitionedTable(
            "p",
            OptimizerTestSupport.fields("f1", INT, "f2", INT, "f3", INT),
            Collections.singletonList(new MapTableIndex("idx1", IndexType.SORTED, Collections.singletonList(0))),
            100
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void testAnd() {
        assertPlan(
            optimizePhysical("SELECT f1, f2 FROM p WHERE 1 < f1 AND f2 < 3 AND f3 = 5"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 3.8),
                planRow(1, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]], index=[idx1], indexExp=[<(1, $0)], remainderExp=[AND(<($1, 3), =($2, 5))]", 3.8)
            )
        );
    }
}
