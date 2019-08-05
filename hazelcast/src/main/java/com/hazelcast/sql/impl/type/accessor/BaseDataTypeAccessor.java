package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;

public interface BaseDataTypeAccessor {
    byte getByte(Object val);
    short getShort(Object val);
    int getInt(Object val);
    long getLong(Object val);
    BigDecimal getDecimal(Object val);
    float getFloat(Object val);
    double getDouble(Object val);
    String getString(Object val);
}
