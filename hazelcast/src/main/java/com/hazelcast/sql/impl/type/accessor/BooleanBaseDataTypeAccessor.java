package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;

public class BooleanBaseDataTypeAccessor implements BaseDataTypeAccessor {
    @Override
    public byte getByte(Object val) {
        return convert(val) ? (byte)1 : (byte)0;
    }

    @Override
    public BigDecimal getDecimal(Object val) {
        return convert(val) ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    private boolean convert(Object val) {
        return (Boolean)val;
    }
}
