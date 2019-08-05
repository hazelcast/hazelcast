package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;

public final class IntegerBaseDataTypeAccessor implements BaseDataTypeAccessor {
    @Override
    public final byte getByte(Object val) {
        return (byte)convert(val);
    }

    @Override
    public final short getShort(Object val) {
        return (short)convert(val);
    }

    @Override
    public final int getInt(Object val) {
        return convert(val);
    }

    @Override
    public final long getLong(Object val) {
        return convert(val);
    }

    @Override
    public final BigDecimal getDecimal(Object val) {
        return new BigDecimal(convert(val));
    }

    @Override
    public final float getFloat(Object val) {
        return convert(val);
    }

    @Override
    public final double getDouble(Object val) {
        return convert(val);
    }

    @Override
    public final String getString(Object val) {
        return Integer.toString(convert(val));
    }

    private int convert(Object val) {
        return (int)val;
    }
}
