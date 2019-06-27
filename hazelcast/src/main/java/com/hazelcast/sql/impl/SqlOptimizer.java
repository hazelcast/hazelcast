package com.hazelcast.sql.impl;

import com.hazelcast.internal.query.physical.PhysicalPlan;

/**
 * Optimizer responsible for conversion of SQL string to executable plan.
 */
public interface SqlOptimizer {
    /**
     * Prepare SQL query.
     *
     * @param sql SQL.
     * @return Executable plan.
     */
    PhysicalPlan prepare(String sql);
}
