package com.hazelcast.sql.impl.type;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Date;

/**
 * Converter for specific type.
 */
public abstract class GenericTypeConverter {

    protected final GenericType sourceType;

    protected GenericTypeConverter(GenericType sourceType) {
        this.sourceType = sourceType;
    }

    public abstract byte asBit(Object val);
    public abstract byte asTinyInt(Object val);
    public abstract short asSmallInt(Object val);
    public abstract int asInt(Object val);
    public abstract long asBigInt(Object val);
    public abstract BigDecimal asDecimal(Object val);
    public abstract float asReal(Object val);
    public abstract double asDouble(Object val);
    public abstract String asVarchar(Object val);
    public abstract LocalDate asDate(Object val);
}
