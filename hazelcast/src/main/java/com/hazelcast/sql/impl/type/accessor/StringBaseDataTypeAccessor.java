package com.hazelcast.sql.impl.type.accessor;

import com.hazelcast.sql.HazelcastSqlException;

import java.math.BigDecimal;

public class StringBaseDataTypeAccessor implements BaseDataTypeAccessor {
    @Override
    public final byte getByte(Object val) {
        try {
            return Byte.valueOf(convert(val));
        }
        catch (NumberFormatException e) {
            throw new HazelcastSqlException(-1, "Cannot convert to byte.");
        }
    }

    @Override
    public final short getShort(Object val) {
        try {
            return Short.valueOf(convert(val));
        }
        catch (NumberFormatException e) {
            throw new HazelcastSqlException(-1, "Cannot convert to byte.");
        }
    }

    @Override
    public final int getInt(Object val) {
        try {
            return Integer.valueOf(convert(val));
        }
        catch (NumberFormatException e) {
            throw new HazelcastSqlException(-1, "Cannot convert to byte.");
        }
    }

    @Override
    public final long getLong(Object val) {
        try {
            return Long.valueOf(convert(val));
        }
        catch (NumberFormatException e) {
            throw new HazelcastSqlException(-1, "Cannot convert to byte.");
        }
    }

    @Override
    public final BigDecimal getDecimal(Object val) {
        try {
            return new BigDecimal(convert(val));
        }
        catch (NumberFormatException e) {
            throw new HazelcastSqlException(-1, "Cannot convert to byte.");
        }
    }

    @Override
    public final float getFloat(Object val) {
        try {
            return Float.valueOf(convert(val));
        }
        catch (NumberFormatException e) {
            throw new HazelcastSqlException(-1, "Cannot convert to byte.");
        }
    }

    @Override
    public final double getDouble(Object val) {
        try {
            return Double.valueOf(convert(val));
        }
        catch (NumberFormatException e) {
            throw new HazelcastSqlException(-1, "Cannot convert to byte.");
        }
    }

    @Override
    public final String getString(Object val) {
        return convert(val);
    }

    private String convert(Object val) {
        return (String)val;
    }
}
