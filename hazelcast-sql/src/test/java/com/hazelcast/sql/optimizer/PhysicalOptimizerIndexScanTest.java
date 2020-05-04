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

package com.hazelcast.sql.optimizer;

import com.hazelcast.config.IndexType;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.optimizer.support.PhysicalOptimizerTestSupport;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;

/**
 * Tests for physical index path selection.
 */
public class PhysicalOptimizerIndexScanTest extends PhysicalOptimizerTestSupport {
    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = partitionedTable(
            "p",
            fields("f1", INT, "f2", INT, "f3", INT),
            Collections.singletonList(new MapTableIndex("idx1", IndexType.SORTED, list(0))),
            100
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void testAnd() {
        RelNode rootInput = optimizePhysical("SELECT f1, f2 FROM p WHERE 1 < f1 AND f2 < 3 AND f3 = 5");

        System.out.println("Done");
    }
}
