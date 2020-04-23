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

/**
 * Utility methods for cost estimation.
 */
public final class CostUtils {
    /** Multiplier to be applied to a CPU cost a project outside a scan operator. */
    public static final double CPU_PROJECT_OUTSIDE_MULTIPLIER = 1.0d;

    /** Multiplier to be applied to a CPU cost a project inside a scan operator. */
    public static final double CPU_PROJECT_INSIDE_MULTIPLIER = 0.8d;

    private CostUtils() {
        // No-op.
    }

    /**
     * Adjust cost of a project operation based on whether it is located in a separate project operator, or inlined into scan.
     * Outside project is more expensive.
     *
     * @param cpu CPU.
     * @param inside {@code True} if project is located inside scan.
     * @return Adjusted cost.
     */
    public static double adjustProjectCpu(double cpu, boolean inside) {
        double multiplier = inside ? CPU_PROJECT_INSIDE_MULTIPLIER : CPU_PROJECT_OUTSIDE_MULTIPLIER;

        return cpu * multiplier;
    }
}
