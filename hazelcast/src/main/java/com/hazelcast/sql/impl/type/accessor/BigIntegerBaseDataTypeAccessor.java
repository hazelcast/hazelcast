package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class BigIntegerBaseDataTypeAccessor implements BaseDataTypeAccessor {
    @Override
    public final byte getByte(Object val) {
        return convert(val).byteValue();
    }

    @Override
    public final short getShort(Object val) {
        return convert(val).shortValue();
    }

    @Override
    public final int getInt(Object val) {
        return convert(val).intValue();
    }

    @Override
    public final long getLong(Object val) {
        return convert(val).longValue();
    }

    @Override
    public final BigDecimal getDecimal(Object val) {
        return new BigDecimal(convert(val));
    }

    @Override
    public final float getFloat(Object val) {
        return convert(val).floatValue();
    }

    @Override
    public final double getDouble(Object val) {
        return convert(val).doubleValue();
    }

    @Override
    public final String getString(Object val) {
        return convert(val).toString();
    }

    private BigInteger convert(Object val) {
        return (BigInteger)val;
    }
}
