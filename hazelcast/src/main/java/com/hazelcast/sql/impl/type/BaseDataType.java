package com.hazelcast.sql.impl.type;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Base data type which is mapped to concrete Java class.
 */
public class BaseDataType {
    /** Precedence of LATE data type. */
    public static final int PRECEDENCE_LATE = 0;

    /** Precedence of BIT data type. */
    public static final int PRECEDENCE_BIT = 1;

    /** Precedence of BYTE data type. */
    public static final int PRECEDENCE_BYTE = 2;

    /** Precedence of SHORT data type. */
    public static final int PRECEDENCE_SHORT = 3;

    /** Precedence of INTEGER data type. */
    public static final int PRECEDENCE_INTEGER = 4;

    /** Precedence of LONG data type. */
    public static final int PRECEDENCE_LONG = 5;

    /** Precedence of BIG_INTEGER data type. */
    public static final int PRECEDENCE_BIG_INTEGER = 6;

    /** Precedence of BIG_DECIMAL data type. */
    public static final int PRECEDENCE_BIG_DECIMAL = 7;

    /** Precedence of FLOAT data type. */
    public static final int PRECEDENCE_FLOAT = 8;

    /** Precedence of DOUBLE data type. */
    public static final int PRECEDENCE_DOUBLE = 9;

    /** Precedence of STRING data type. */
    public static final int PRECEDENCE_STRING = 1;

    public static final BaseDataType LATE = new BaseDataType(
        BaseDataTypeGroup.LATE,
        PRECEDENCE_LATE,
        null
    );

    public static final BaseDataType BIT = new BaseDataType(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_BIT,
        Boolean.class
    );

    public static final BaseDataType BYTE = new BaseDataType(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_BYTE,
        Byte.class
    );

    public static final BaseDataType SHORT = new BaseDataType(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_SHORT,
        Short.class
    );

    public static final BaseDataType INTEGER = new BaseDataType(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_INTEGER,
        Integer.class
    );

    public static final BaseDataType LONG = new BaseDataType(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_LONG,
        Long.class
    );

    public static final BaseDataType BIG_INTEGER  = new BaseDataType(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_BIG_INTEGER,
        BigInteger.class
    );

    public static final BaseDataType BIG_DECIMAL  = new BaseDataType(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_BIG_DECIMAL,
        BigDecimal.class
    );

    public static final BaseDataType FLOAT  = new BaseDataType(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_FLOAT,
        Float.class
    );

    public static final BaseDataType DOUBLE = new BaseDataType(
        BaseDataTypeGroup.NUMERIC,
        PRECEDENCE_DOUBLE,
        Double.class
    );

    public static final BaseDataType STRING = new BaseDataType(
        BaseDataTypeGroup.STRING,
        PRECEDENCE_STRING,
        String.class
    );

    /** Data type group. */
    private final BaseDataTypeGroup group;

    /** Precedence. */
    private final int precedence;

    /** Underlying class. */
    private final Class clazz;

    private BaseDataType(BaseDataTypeGroup group, int precedence, Class clazz) {
        this.group = group;
        this.precedence = precedence;
        this.clazz = clazz;
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
