package com.hazelcast.sql.impl;

import com.hazelcast.internal.query.physical.PhysicalPlan;

/**
 * No-op optimizer.
 */
public class SqlNoopOptimizer implements SqlOptimizer {
    @Override
    public PhysicalPlan prepare(String sql) {
        // TODO: Proper exception type and error message.
        throw new UnsupportedOperationException("Cannot execute SQL query because hazelcast-sql module is not " +
            "in the classpath.");
    }
}
