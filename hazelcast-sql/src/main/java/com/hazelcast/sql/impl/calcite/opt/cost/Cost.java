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
import org.apache.calcite.plan.RelOptUtil;

import java.util.Objects;

/**
 * Cost of relational operator.
 */
public class Cost implements RelOptCost {
    /** Zero cost. */
    public static final Cost ZERO = new Cost(
        0.0d, 0.0d, 0.0d
    );

    /** Tiny cost. */
    public static final Cost TINY = new Cost(
        1.0d, 1.0d, 0.0d
    );

    /** Huge cost. */
    public static final Cost HUGE = new Cost(
        Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE
    );

    /** Infinite cost. */
    public static final Cost INFINITY = new Cost(
        Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY
    );

    /** Number of rows returned. */
    private final double rows;

    /** CPU cycles. */
    private final double cpu;

    /** Amount of data transferred over network. */
    private final double io;

    Cost(double rows, double cpu, double io) {
        this.rows = rows;
        this.cpu = cpu;
        this.io = io;
    }

    @Override
    public double getRows() {
        return rows;
    }

    @Override
    public double getCpu() {
        return cpu;
    }

    @Override
    public double getIo() {
        return io;
    }

    @Override
    public boolean isInfinite() {
        // Cost is considered infinite if any of its components is infinite.
        return this == INFINITY
            || rows == Double.POSITIVE_INFINITY
            || cpu == Double.POSITIVE_INFINITY
            || io == Double.POSITIVE_INFINITY;
    }

    @Override
    public boolean isEqWithEpsilon(RelOptCost other) {
        if (!(other instanceof Cost)) {
            return false;
        }

        Cost other0 = (Cost) other;

        if (this == other0) {
            return true;
        }

        return Math.abs(rows - other0.rows) < RelOptUtil.EPSILON
            && Math.abs(cpu - other0.cpu) < RelOptUtil.EPSILON
            && Math.abs(io - other0.io) < RelOptUtil.EPSILON;
    }

    @Override
    public boolean isLe(RelOptCost other) {
        Cost other0 = (Cost) other;

        if (equals(other0)) {
            return true;
        }

        return (rows <= other0.rows)
            && (cpu <= other0.cpu)
            && (io <= other0.io);
    }

    @Override
    public boolean isLt(RelOptCost other) {
        return isLe(other) && !equals(other);
    }

    @Override
    public RelOptCost plus(RelOptCost other) {
        Cost other0 = (Cost) other;

        if ((this == INFINITY) || (other0 == INFINITY)) {
            return INFINITY;
        }

        return new Cost(
            rows + other0.rows,
            cpu + other0.cpu,
            io + other0.io
        );
    }

    @Override
    public RelOptCost minus(RelOptCost other) {
        if (this == INFINITY) {
            return this;
        }

        Cost other0 = (Cost) other;

        return new Cost(
            rows - other0.rows,
            cpu - other0.cpu,
            io - other0.io
        );
    }

    @Override
    public RelOptCost multiplyBy(double factor) {
        if ((this == INFINITY)) {
            return INFINITY;
        }

        return new Cost(
            rows * factor,
            cpu * factor,
            io * factor
        );
    }

    @Override
    public double divideBy(RelOptCost cost) {
        // Use the same approach as in Calcite's VolcanoCost.
        Cost that = (Cost) cost;

        double d = 1;
        double n = 0;

        if (qualifiesForDivide(rows, that.rows)) {
            d *= rows / that.rows;
            n++;
        }
        if (qualifiesForDivide(cpu, that.cpu)) {
            d *= cpu / that.cpu;
            n++;
        }
        if (qualifiesForDivide(io, that.io)) {
            d *= io / that.io;
            n++;
        }

        if (n == 0) {
            return 1.0;
        }

        return Math.pow(d, 1 / n);
    }

    private static boolean qualifiesForDivide(double first, double second) {
        return first != 0 && !Double.isInfinite(first) && (second != 0) && !Double.isInfinite(second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rows, cpu, io);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Cost) {
            return equals((RelOptCost) other);
        }

        return equals((RelOptCost) other);
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
            && Double.compare(other0.io, io) == 0;
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
            content = "rows=" + rows + ", cpu=" + cpu + ", io=" + io;
        }

        return getClass().getSimpleName() + '{' + content + '}';
    }
}
