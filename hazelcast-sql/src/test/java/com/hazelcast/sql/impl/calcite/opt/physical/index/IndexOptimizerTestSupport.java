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

package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.sql.impl.calcite.opt.PlanRows;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.RelNode;

import static org.junit.Assert.assertEquals;

/**
 * Helper methods for index-related optimizer tests.
 */
public class IndexOptimizerTestSupport extends OptimizerTestSupport {
    protected static PlanRows planWithIndex(String indexName, String indexExp, String remainderExp) {
        return plan(
                planRow(0, RootPhysicalRel.class, ""),
                planRow(1, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0]]]], index=[" + indexName
                        + "], indexExp=[" + indexExp + "], remainderExp=[" + remainderExp + "]")
        );
    }

    protected void checkIndex(
            String sql,
            String expectedIndex,
            String expectedIndexFilter,
            String expectedRemainderFilter,
            QueryDataType... parameterTypes
    ) {
        // Build actual plan
        RelNode actualRel = optimizePhysical(sql, parameterTypes);
        PlanRows actualPlan = plan(actualRel);

        // Build expected plan
        PlanRows expectedPlan = planWithIndex(expectedIndex, expectedIndexFilter, expectedRemainderFilter);

        // Check
        assertPlan(actualPlan, expectedPlan);
    }

    protected void checkNoIndex(String sql, QueryDataType... parameterTypes) {
        RelNode rel = optimizePhysical(sql, parameterTypes);
        PlanRows plan = plan(rel);

        assertEquals(2, plan.getRowCount());
        assertEquals(MapScanPhysicalRel.class.getSimpleName(), plan.getRow(1).getNode());
    }
}
