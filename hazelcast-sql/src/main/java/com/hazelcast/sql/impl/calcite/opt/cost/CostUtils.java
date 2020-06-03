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

package com.hazelcast.sql.impl.calcite.opt.cost;

import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * Utility methods for cost estimation.
 */
public final class CostUtils {
    /** CPU multiplier applied to a project/filter inside of a scan operator. */
    public static final double CONSTRAINED_SCAN_CPU_MULTIPLIER = 0.8d;

    /** CPU multiplier applied to normal scan. */
    public static final double TABLE_SCAN_CPU_MULTIPLIER = 1.0d;

    /** Multiplier for the CPU part of the cost. Assumes 1ns per item. */
    public static final double CPU_COST_MULTIPLIER = 1.0d;

    /** Multiplier for the network part of the cost. Assumes ~10µs per 1Kb that results in ~10ns per byte. */
    public static final double NETWORK_COST_MULTIPLIER = CPU_COST_MULTIPLIER * 10;

    /** Replacement value if filter selectivity cannot be determined.  */
    private static final double UNKNOWN_SELECTIVITY = 0.25d;

    private CostUtils() {
        // No-op.
    }

    /**
     * Adjust the cost of a CPU-related operation (project, filter) located inside a scan. This allows the optimizer to prefer
     * filters and projects inlined into the scan.
     *
     * @param cpu CPU.
     * @return Adjusted cost.
     */
    public static double adjustCpuForConstrainedScan(double cpu) {
        return cpu * CONSTRAINED_SCAN_CPU_MULTIPLIER;
    }

    /**
     * Adjust row count based on filter selectivity.
     *
     * @param rowCount Row count.
     * @param selectivity Selectivity.
     * @return New row count.
     */
    public static double adjustFilteredRowCount(double rowCount, Double selectivity) {
        if (selectivity == null) {
            selectivity = UNKNOWN_SELECTIVITY;
        }

        return rowCount * selectivity;
    }

    public static double getProjectCpu(double rowCount, int expressionCount) {
        return rowCount * expressionCount;
    }

    public static int getEstimatedRowWidth(RelNode rel) {
        int res = 0;

        for (RelDataTypeField field : rel.getRowType().getFieldList()) {
            res += SqlToQueryType.map(field.getType().getSqlTypeName()).getTypeFamily().getEstimatedSize();
        }

        return res;
    }
}
