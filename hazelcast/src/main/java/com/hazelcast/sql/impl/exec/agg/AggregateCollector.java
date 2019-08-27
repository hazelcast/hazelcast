package com.hazelcast.sql.impl.exec.agg;

import java.util.HashSet;
import java.util.Set;

/**
 * Stored value of a single aggregate.
 */
public abstract class AggregateCollector {
    /** Set of distinct values. */
    private final Set<Object> distinctSet;

    protected AggregateCollector(boolean distinct) {
        this.distinctSet = distinct ? new HashSet<>() : null;
    }

    public void collect(Object value) {
        if (distinctSet != null && !distinctSet.add(value))
            return;

        collect0(value);
    }

    protected abstract void collect0(Object value);

    public abstract Object reduce();

    // TODO: Not needed on the general interface level.
    public abstract void reset();
}
