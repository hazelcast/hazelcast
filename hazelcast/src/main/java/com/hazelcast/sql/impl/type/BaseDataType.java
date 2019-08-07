package com.hazelcast.sql.impl.type;

import com.hazelcast.sql.impl.type.accessor.Converter;
import com.hazelcast.sql.impl.type.accessor.BigDecimalConverter;
import com.hazelcast.sql.impl.type.accessor.BigIntegerConverter;
import com.hazelcast.sql.impl.type.accessor.BooleanConverter;
import com.hazelcast.sql.impl.type.accessor.ByteConverter;
import com.hazelcast.sql.impl.type.accessor.DoubleConverter;
import com.hazelcast.sql.impl.type.accessor.FloatConverter;
import com.hazelcast.sql.impl.type.accessor.IntegerConverter;
import com.hazelcast.sql.impl.type.accessor.LongConverter;
import com.hazelcast.sql.impl.type.accessor.ShortConverter;
import com.hazelcast.sql.impl.type.accessor.StringConverter;
import com.hazelcast.sql.impl.type.accessor.TimestampBaseDataTypeAccessor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import static com.hazelcast.sql.impl.type.TypeUtils.PRECEDENCE_BIG_DECIMAL;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECEDENCE_BIG_INTEGER;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECEDENCE_BOOLEAN;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECEDENCE_BYTE;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECEDENCE_DOUBLE;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECEDENCE_FLOAT;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECEDENCE_INTEGER;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECEDENCE_LATE;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECEDENCE_LONG;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECEDENCE_SHORT;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECEDENCE_STRING;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECEDENCE_TIMESTAMP;

/**
 * Base data type which is mapped to concrete Java class.
 */
public enum BaseDataType {
    LATE(
        BaseDataTypeGroup.LATE,
        PRECEDENCE_LATE,
        null,
        null
    ),

    BOOLEAN(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_BOOLEAN,
        Boolean.class,
        new BooleanConverter()
    ),

    BYTE(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_BYTE,
        Byte.class,
        new ByteConverter()
    ),

    SHORT(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_SHORT,
        Short.class,
        new ShortConverter()
    ),

    INTEGER(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_INTEGER,
        Integer.class,
        new IntegerConverter()
    ),

    LONG(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_LONG,
        Long.class,
        new LongConverter()
    ),

    BIG_INTEGER(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_BIG_INTEGER,
        BigInteger.class,
        new BigIntegerConverter()
    ),

    BIG_DECIMAL(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_BIG_DECIMAL,
        BigDecimal.class,
        new BigDecimalConverter()
    ),

    FLOAT(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_FLOAT,
        Float.class,
        new FloatConverter()
    ),

    DOUBLE(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_DOUBLE,
        Double.class,
        new DoubleConverter()
    ),

    STRING(
        BaseDataTypeGroup.STRING,
        PRECEDENCE_STRING,
        String.class,
        new StringConverter()
    ),

    TIMESTAMP(
        BaseDataTypeGroup.DATETIME,
        PRECEDENCE_TIMESTAMP,
        Date.class,
        new TimestampBaseDataTypeAccessor()
    );

    /** Data type group. */
    private final BaseDataTypeGroup group;

    /** Precedence. */
    private final int precedence;

    /** Underlying class. */
    private final Class clazz;

    /** Accessor. */
    private final Converter accessor;

    BaseDataType(BaseDataTypeGroup group, int precedence, Class clazz, Converter accessor) {
        this.group = group;
        this.precedence = precedence;
        this.clazz = clazz;
        this.accessor = accessor;
    }

    public BaseDataTypeGroup getGroup() {
        return group;
    }

    public int getPrecedence() {
        return precedence;
    }

    public Class getClazz() {
        return clazz;
    }

    public Converter getAccessor() {
        return accessor;
    }

    /**
     * Check if provided object is of the same base data type.
     *
     * @param obj Object.
     * @return {@code True} if object is of the same data type.
     */
    public boolean isSame(Object obj) {
        if (obj == null)
            return true;

        return obj.getClass() == clazz;
    }
}
