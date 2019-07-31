package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;
import java.math.BigInteger;

public class BigDecimalBaseDataTypeAccessor implements BaseDataTypeAccessor {
    @Override
    public byte getByte(Object val) {
        return convert(val).byteValue();
    }

    @Override
    public BigDecimal getDecimal(Object val) {
        return convert(val);
    }

    private BigDecimal convert(Object val) {
        return (BigDecimal)val;
    }
}
