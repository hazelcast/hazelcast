package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.impl.type.DataType;

/**
 * Temporal value obtained through string parsing.
 */
public class TemporalValue {
    /** Type. */
    private final DataType type;

    /** Value. */
    private final Object val;

    public TemporalValue(DataType type, Object val) {
        this.type = type;
        this.val = val;
    }

    public DataType getType() {
        return type;
    }

    public Object getValue() {
        return val;
    }
}
