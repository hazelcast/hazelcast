package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;

public class IntegerBaseDataTypeAccessor implements BaseDataTypeAccessor {
    @Override
    public byte getByte(Object val) {
        return (byte)convert(val);
    }

    @Override
    public BigDecimal getDecimal(Object val) {
        return new BigDecimal(convert(val));
    }

    private int convert(Object val) {
        return (int)val;
    }
}
