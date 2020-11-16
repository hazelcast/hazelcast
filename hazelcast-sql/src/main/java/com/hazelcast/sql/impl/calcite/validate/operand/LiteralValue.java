package com.hazelcast.sql.impl.calcite.validate.operand;

import com.hazelcast.sql.impl.type.QueryDataType;

public final class LiteralValue {

    public static final LiteralValue BOOLEAN_TRUE = new LiteralValue(QueryDataType.BOOLEAN, true);
    public static final LiteralValue BOOLEAN_FALSE = new LiteralValue(QueryDataType.BOOLEAN, false);

    private final QueryDataType type;
    private final Object value;

    LiteralValue(QueryDataType type, Object value) {
        this.value = value;
        this.type = type;
    }

    public QueryDataType getType() {
        return type;
    }

    @SuppressWarnings("unchecked")
    public <T> T getValue() {
        return (T) value;
    }
}
