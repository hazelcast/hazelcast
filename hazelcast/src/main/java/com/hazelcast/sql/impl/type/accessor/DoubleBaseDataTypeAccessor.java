package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;

public class DoubleBaseDataTypeAccessor implements BaseDataTypeAccessor {
    @Override
    public byte getByte(Object val) {
        return (byte)convert(val);
    }

    @Override
    public BigDecimal getDecimal(Object val) {
        return new BigDecimal(convert(val));
    }

    private double convert(Object val) {
        return (double)val;
    }
}
