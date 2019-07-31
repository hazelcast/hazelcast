package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;
import java.math.BigInteger;

public class BigIntegerBaseDataTypeAccessor implements BaseDataTypeAccessor {
    @Override
    public byte getByte(Object val) {
        return convert(val).byteValue();
    }

    @Override
    public BigDecimal getDecimal(Object val) {
        return new BigDecimal(convert(val));
    }

    private BigInteger convert(Object val) {
        return (BigInteger)val;
    }
}
