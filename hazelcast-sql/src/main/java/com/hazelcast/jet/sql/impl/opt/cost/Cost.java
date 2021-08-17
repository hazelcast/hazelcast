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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;

/**
 * Cost of relational operator.
 * <p>
 * We use our own implementation instead of the one provided by Apache Calcite. In Apache Calcite, the cost is a vector of
 * three values - row count, CPU and IO. First, it has some problems with comparison semantics [1]. Second, its comparison
 * depends mostly on row count, while in our case other factors, such as network, are more important. Last, it has a
 * number of methods and variables that are otherwise unused (or mostly unused).
 * <p>
 * Our implementation still tracks row count, CPU and network, but it doesn't implement unnecessary methods, has proper
 * comparison semantics, and use CPU and network for cost comparison instead row count.
 * <p>
 * [1] https://issues.apache.org/jira/browse/CALCITE-3956
 */
public class Cost implements RelOptCost {

    public static final Cost ZERO = new Cost(0.0d, 0.0d, 0.0d);
    public static final Cost TINY = new Cost(1.0d, 1.0d, 0.0d);
    public static final Cost HUGE = new Cost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);
    public static final Cost INFINITY = new Cost(Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);

    private final double rows;
    private final double cpu;
    private final double network;

    Cost(double rows, double cpu, double network) {
        this.rows = rows;
        this.cpu = cpu;
        this.network = network;
    }

    public double getRowsInternal() {
        return rows;
    }

    public double getCpuInternal() {
        return cpu;
    }

    public double getNetworkInternal() {
        return network;
    }

    @Override
    public double getRows() {
        // Make sure that Calcite doesn't rely on our values.
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public double getCpu() {
        // Make sure that Calcite doesn't rely on our values.
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public double getIo() {
        // Make sure that Calcite doesn't rely on our values.
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public boolean isInfinite() {
        return cpu == Double.POSITIVE_INFINITY || network == Double.POSITIVE_INFINITY;
    }

    @Override
    public boolean isEqWithEpsilon(RelOptCost other) {
        if (!(other instanceof Cost)) {
            return false;
        }

        Cost other0 = (Cost) other;

        if (isInfinite() || other0.isInfinite()) {
            return false;
        }

        if (this == other0) {
            return true;
        }

        return Math.abs(getValue() - other0.getValue()) < RelOptUtil.EPSILON;
    }

    @Override
    public boolean isLe(RelOptCost other) {
        Cost other0 = (Cost) other;

        if (equals(other0)) {
            return true;
        }

        return getValue() <= other0.getValue();
    }

    @Override
    public boolean isLt(RelOptCost other) {
        return isLe(other) && !equals(other);
    }

    @Override
    public Cost plus(RelOptCost other) {
        Cost other0 = (Cost) other;

        if (isInfinite() || other.isInfinite()) {
            return INFINITY;
        }

        return new Cost(rows + other0.rows, cpu + other0.cpu, network + other0.network);
    }

    @Override
    public RelOptCost minus(RelOptCost other) {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public Cost multiplyBy(double factor) {
        if (isInfinite()) {
            return INFINITY;
        }

        return new Cost(rows * factor, cpu * factor, network * factor);
    }

    @Override
    public double divideBy(RelOptCost cost) {
        throw new UnsupportedOperationException("Should not be called.");
    }

    private double getValue() {
        if (isInfinite()) {
            return Double.POSITIVE_INFINITY;
        }

        return cpu * CostUtils.CPU_COST_MULTIPLIER + network * CostUtils.NETWORK_COST_MULTIPLIER;
    }

    @Override
    public int hashCode() {
        int res = Double.hashCode(rows);

        res += 31 * Double.hashCode(cpu);
        res += 31 * Double.hashCode(network);

        return res;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Cost) {
            return equals((RelOptCost) other);
        }

        return false;
    }

    @Override
    public boolean equals(RelOptCost other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof Cost)) {
            return false;
        }

        Cost other0 = (Cost) other;

        return Double.compare(other0.rows, rows) == 0
                && Double.compare(other0.cpu, cpu) == 0
                && Double.compare(other0.network, network) == 0;
    }

    @Override
    public String toString() {
        String content;

        if (equals(INFINITY)) {
            content = "infinity";
        } else if (equals(HUGE)) {
            content = "huge";
        } else if (equals(TINY)) {
            content = "tiny";
        } else if (equals(ZERO)) {
            content = "zero";
        } else {
            content = "rows=" + rows + ", cpu=" + cpu + ", network=" + network;
        }

        return getClass().getSimpleName() + '{' + content + '}';
    }
}
