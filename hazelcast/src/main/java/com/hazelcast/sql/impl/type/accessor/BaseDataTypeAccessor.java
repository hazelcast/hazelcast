package com.hazelcast.sql.impl.type.accessor;

import java.math.BigDecimal;

public interface BaseDataTypeAccessor {
    byte getByte(Object val);
    BigDecimal getDecimal(Object val);
}
