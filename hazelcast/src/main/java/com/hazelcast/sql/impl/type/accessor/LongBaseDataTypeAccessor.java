package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;

public class LongBaseDataTypeAccessor implements BaseDataTypeAccessor {
    @Override
    public byte getByte(Object val) {
        return (byte)convert(val);
    }

    @Override
    public BigDecimal getDecimal(Object val) {
        return new BigDecimal(convert(val));
    }

    private long convert(Object val) {
        return (Long)val;
    }
}
