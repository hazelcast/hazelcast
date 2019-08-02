package com.hazelcast.sql.impl.type;

import com.hazelcast.sql.impl.type.accessor.BaseDataTypeAccessor;
import com.hazelcast.sql.impl.type.accessor.BigDecimalBaseDataTypeAccessor;
import com.hazelcast.sql.impl.type.accessor.BigIntegerBaseDataTypeAccessor;
import com.hazelcast.sql.impl.type.accessor.BooleanBaseDataTypeAccessor;
import com.hazelcast.sql.impl.type.accessor.ByteBaseDataTypeAccessor;
import com.hazelcast.sql.impl.type.accessor.DoubleBaseDataTypeAccessor;
import com.hazelcast.sql.impl.type.accessor.FloatBaseDataTypeAccessor;
import com.hazelcast.sql.impl.type.accessor.IntegerBaseDataTypeAccessor;
import com.hazelcast.sql.impl.type.accessor.LongBaseDataTypeAccessor;
import com.hazelcast.sql.impl.type.accessor.ShortBaseDataTypeAccessor;

import java.math.BigDecimal;
import java.math.BigInteger;

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
        new BooleanBaseDataTypeAccessor()
    ),

    BYTE(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_BYTE,
        Byte.class,
        new ByteBaseDataTypeAccessor()
    ),

    SHORT(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_SHORT,
        Short.class,
        new ShortBaseDataTypeAccessor()
    ),

    INTEGER(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_INTEGER,
        Integer.class,
        new IntegerBaseDataTypeAccessor()
    ),

    LONG(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_LONG,
        Long.class,
        new LongBaseDataTypeAccessor()
    ),

    BIG_INTEGER(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_BIG_INTEGER,
        BigInteger.class,
        new BigIntegerBaseDataTypeAccessor()
    ),

    BIG_DECIMAL(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_BIG_DECIMAL,
        BigDecimal.class,
        new BigDecimalBaseDataTypeAccessor()
    ),

    FLOAT(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_FLOAT,
        Float.class,
        new FloatBaseDataTypeAccessor()
    ),

    DOUBLE(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_DOUBLE,
        Double.class,
        new DoubleBaseDataTypeAccessor()
    ),

    STRING(
        BaseDataTypeGroup.STRING,
        PRECEDENCE_STRING,
        String.class,
        null
    );

    /** Data type group. */
    private final BaseDataTypeGroup group;

    /** Precedence. */
    private final int precedence;

    /** Underlying class. */
    private final Class clazz;

    /** Accessor. */
    private final BaseDataTypeAccessor accessor;

    private BaseDataType(BaseDataTypeGroup group, int precedence, Class clazz, BaseDataTypeAccessor accessor) {
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

    public BaseDataTypeAccessor getAccessor() {
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
