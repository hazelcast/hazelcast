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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;

/**
 * Cost factory which is injected into Calcite optimizer.
 */
public final class CostFactory implements RelOptCostFactory {
    /** Singleton instance. */
    public static final CostFactory INSTANCE = new CostFactory();

    private CostFactory() {
        // No-op.
    }

    @Override
    public RelOptCost makeCost(double rowCount, double cpu, double io) {
        return new Cost(rowCount, cpu, io);
    }

    @Override
    public RelOptCost makeHugeCost() {
        return Cost.HUGE;
    }

    @Override
    public RelOptCost makeInfiniteCost() {
        return Cost.INFINITY;
    }

    @Override
    public RelOptCost makeTinyCost() {
        return Cost.TINY;
    }

    @Override
    public RelOptCost makeZeroCost() {
        return Cost.ZERO;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{}";
    }
}
