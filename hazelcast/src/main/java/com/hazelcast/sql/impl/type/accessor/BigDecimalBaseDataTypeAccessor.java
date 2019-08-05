package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;

public final class BigDecimalBaseDataTypeAccessor implements BaseDataTypeAccessor {
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
        return convert(val);
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

    private BigDecimal convert(Object val) {
        return (BigDecimal)val;
    }
}
