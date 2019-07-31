package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;

public class ByteBaseDataTypeAccessor implements BaseDataTypeAccessor {
    @Override
    public byte getByte(Object val) {
        return convert(val);
    }

    @Override
    public BigDecimal getDecimal(Object val) {
        return new BigDecimal(convert(val));
    }

    private byte convert(Object val) {
        return (byte)val;
    }
}
