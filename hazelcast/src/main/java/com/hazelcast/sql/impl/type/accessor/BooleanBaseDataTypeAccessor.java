package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;

public final class BooleanBaseDataTypeAccessor implements BaseDataTypeAccessor {
    @Override
    public final byte getByte(Object val) {
        return convert(val);
    }

    @Override
    public final short getShort(Object val) {
        return convert(val);
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
        return convert(val) == 0 ? BigDecimal.ZERO : BigDecimal.ONE;
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
        return Byte.toString(convert(val));
    }

    private byte convert(Object val) {
        return (Boolean)val ? (byte)1 : (byte)0;
    }
}
