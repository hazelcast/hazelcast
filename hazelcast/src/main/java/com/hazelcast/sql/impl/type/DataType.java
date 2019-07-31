package com.hazelcast.sql.impl.type;

import com.hazelcast.sql.HazelcastSqlException;

/**
 * Data type represents a type of concrete expression which is based on some basic data type.
 */
public class DataType {
    /** Constant: unlimited precision. */
    public static final int PRECISION_UNLIMITED = -1;

    /** Constant: unlimited scale. */
    public static final int SCALE_UNLIMITED = -1;

    /** Precision of BIT. */
    private static final int PRECISION_BIT = 1;

    /** Precision of TINYINT. */
    private static final int PRECISION_TINYINT = 4;

    /** Precision of SMALLINT. */
    private static final int PRECISION_SMALLINT = 7;

    /** Precision of INT. */
    private static final int PRECISION_INT = 11;

    /** Precision of BIGINT */
    private static final int PRECISION_BIGINT = 20;

    /** Late data type. */
    public static final DataType LATE = new DataType(BaseDataType.LATE, PRECISION_UNLIMITED, SCALE_UNLIMITED);

    /** BIT data type. */
    public static final DataType BIT = new DataType(BaseDataType.BIT, PRECISION_BIT, 0);

    /** TINYINT data type. */
    public static final DataType TINYINT = new DataType(BaseDataType.BYTE, PRECISION_TINYINT, 0);

    /** SMALLINT data type. */
    public static final DataType SMALLINT = new DataType(BaseDataType.SHORT, PRECISION_SMALLINT, 0);

    /** INT data type. */
    public static final DataType INT = new DataType(BaseDataType.INTEGER, PRECISION_INT, 0);

    /** BIGINT data type. */
    public static final DataType BIGINT = new DataType(BaseDataType.LONG, PRECISION_BIGINT, 0);

    /** DECIMAL data type. */
    // TODO: No precision calculation because it is expensive.
    public static final DataType DECIMAL = new DataType(BaseDataType.BIG_DECIMAL, PRECISION_UNLIMITED, SCALE_UNLIMITED);

    /** DECIMAL data type created from BigInteger instance. */
    // TODO: No precision calculation because it is expensive.
    public static final DataType DECIMAL_BIGINT = new DataType(BaseDataType.BIG_INTEGER, PRECISION_UNLIMITED, 0);

    /** REAL data type. */
    public static final DataType REAL = new DataType(BaseDataType.FLOAT, PRECISION_UNLIMITED, SCALE_UNLIMITED);

    /** DOUBLE data type. */
    public static final DataType DOUBLE = new DataType(BaseDataType.DOUBLE, PRECISION_UNLIMITED, SCALE_UNLIMITED);

    /** VARCHAR data type. */
    public static final DataType VARCHAR = new DataType(BaseDataType.STRING, PRECISION_UNLIMITED, SCALE_UNLIMITED);

    /** Underlying Java type. */
    private final BaseDataType baseType;

    /** Precision. */
    private final int precision;

    /** Scale. */
    private final int scale;

    /**
     * Get type of the given object.
     *
     * @param obj Object.
     * @return Object's type.
     */
    public static DataType resolveType(Object obj) {
        if (obj == null)
            return LATE;

        Class clazz = obj.getClass();

        if (clazz == BaseDataType.BIT.getClazz())
            return BIT;
        else if (clazz == BaseDataType.BYTE.getClazz())
            return TINYINT;
        else if (clazz == BaseDataType.SHORT.getClazz())
            return SMALLINT;
        else if (clazz == BaseDataType.INTEGER.getClazz())
            return INT;
        else if (clazz == BaseDataType.LONG.getClazz())
            return BIGINT;
        else if (clazz == BaseDataType.BIG_INTEGER.getClazz())
            return DECIMAL_BIGINT;
        else if (clazz == BaseDataType.BIG_DECIMAL.getClazz())
            return DECIMAL;
        else if (clazz == BaseDataType.FLOAT.getClazz())
            return REAL;
        else if (clazz == BaseDataType.DOUBLE.getClazz())
            return DOUBLE;
        else if (clazz == BaseDataType.STRING.getClazz())
            return VARCHAR;

        // TODO: We will have to return "OBJECT" here instead for nested field access.
        throw new HazelcastSqlException(-1, "Unsupported data type: " + clazz.getSimpleName());
    }

    /**
     * Get integer type for the given precision.
     *
     * @param precision Precision.
     * @return Type.
     */
    public static DataType integerType(int precision) {
        // TODO: Cache this in array.
        if (precision == PRECISION_UNLIMITED)
            return new DataType(BaseDataType.BIG_DECIMAL, PRECISION_UNLIMITED, 0);
        else if (precision == PRECISION_BIT)
            return DataType.BIT;
        else if (precision < PRECISION_TINYINT)
            return new DataType(BaseDataType.BYTE, precision, 0);
        else if (precision == PRECISION_TINYINT)
            return DataType.TINYINT;
        else if (precision < PRECISION_SMALLINT)
            return new DataType(BaseDataType.SHORT, precision, 0);
        else if (precision == PRECISION_SMALLINT)
            return DataType.SMALLINT;
        else if (precision < PRECISION_INT)
            return new DataType(BaseDataType.INTEGER, precision, 0);
        else if (precision == PRECISION_INT)
            return DataType.INT;
        else if (precision < PRECISION_BIGINT)
            return new DataType(BaseDataType.LONG, precision, 0);
        else if (precision == PRECISION_BIGINT)
            return DataType.BIGINT;
        else
            return new DataType(BaseDataType.BIG_DECIMAL, PRECISION_UNLIMITED, 0);
    }

    private DataType(BaseDataType baseType, int precision, int scale) {
        this.baseType = baseType;
        this.precision = precision;
        this.scale = scale;
    }

    public BaseDataType getBaseType() {
        return baseType;
    }

    public int getPrecedence() {
        return baseType.getPrecedence();
    }

    public int getPrecision() {
        return precision;
    }

    public boolean hasPrecision() {
        return precision != PRECISION_UNLIMITED;
    }

    public int getScale() {
        return scale;
    }

    public boolean hasScale() {
        return scale != SCALE_UNLIMITED;
    }

    public boolean isSame(Object val) {
        return baseType.isSame(val);
    }

    public void forceSame(Object val) {
        if (!isSame(val))
            throw new HazelcastSqlException(-1, "Invalid type.");
    }

    public boolean isNumeric() {
        return baseType.getGroup() == BaseDataTypeGroup.NUMERIC;
    }
}
