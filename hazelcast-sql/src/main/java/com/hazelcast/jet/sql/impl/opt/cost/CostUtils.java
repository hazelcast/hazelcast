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

package com.hazelcast.jet.sql.impl.opt.cost;

import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
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


    /** CPU multiplier applied to index scan (sorted). */
    public static final double INDEX_SCAN_CPU_MULTIPLIER_SORTED = 1.2d;

    /** CPU multiplier applied to index scan (hash). */
    public static final double INDEX_SCAN_CPU_MULTIPLIER_HASH = 1.1d;

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
     * Get CPU multiplier for index scan. It ensures that normal scans are preferred over index scans when there are no
     * conditions and collation provided by the index is not important for the specific query.
     * <p>
     * We assume that index scan is more expensive than normal scan due to additional level of indirection. This is not the
     * case for covering index scans, but we do not support them yet.
     * <p>
     * We assume that HASH index lookup is cheaper than SORTED index lookup in general case, because the former has O(1)
     * complexity, while the latter has O(N) complexity.
     *
     * @param type Index type.
     * @return CPU multiplier.
     */
    public static double indexScanCpuMultiplier(IndexType type) {
        if (type == IndexType.HASH) {
            return INDEX_SCAN_CPU_MULTIPLIER_HASH;
        } else {
            assert type == IndexType.SORTED;

            return INDEX_SCAN_CPU_MULTIPLIER_SORTED;
        }
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
     * @param rowCount    Row count.
     * @param selectivity Selectivity.
     * @return New row count.
     */
    public static Double adjustFilteredRowCount(Double rowCount, Double selectivity) {
        if (rowCount == null) {
            return null;
        }

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
            res += HazelcastTypeUtils.toHazelcastType(field.getType()).getTypeFamily().getEstimatedSize();
        }

        return res;
    }
}
