package com.hazelcast.sql.impl.type;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;

import static com.hazelcast.sql.impl.type.TypeUtils.PRECISION_BIGINT;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECISION_BIT;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECISION_INT;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECISION_SMALLINT;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECISION_TINYINT;
import static com.hazelcast.sql.impl.type.TypeUtils.PRECISION_UNLIMITED;
import static com.hazelcast.sql.impl.type.TypeUtils.SCALE_UNLIMITED;

/**
 * Data type represents a type of concrete expression which is based on some basic data type.
 */
public class DataType {
    /** Late data type. */
    public static final DataType LATE = new DataType(BaseDataType.LATE, PRECISION_UNLIMITED, SCALE_UNLIMITED);

    /** BOOLEAN data type. */
    public static final DataType BIT = new DataType(BaseDataType.BOOLEAN, PRECISION_BIT, 0);

    /** TINYINT data type. */
    public static final DataType TINYINT = new DataType(BaseDataType.BYTE, PRECISION_TINYINT, 0);

    /** SMALLINT data type. */
    public static final DataType SMALLINT = new DataType(BaseDataType.SHORT, PRECISION_SMALLINT, 0);

    /** INT data type. */
    public static final DataType INT = new DataType(BaseDataType.INTEGER, PRECISION_INT, 0);

    /** BIGINT data type. */
    public static final DataType BIGINT = new DataType(BaseDataType.LONG, PRECISION_BIGINT, 0);

    /** DECIMAL data type. */
    public static final DataType DECIMAL = new DataType(BaseDataType.BIG_DECIMAL, PRECISION_UNLIMITED, SCALE_UNLIMITED);

    /** REAL data type. */
    public static final DataType REAL = new DataType(BaseDataType.FLOAT, PRECISION_UNLIMITED, SCALE_UNLIMITED);

    /** DOUBLE data type. */
    public static final DataType DOUBLE = new DataType(BaseDataType.DOUBLE, PRECISION_UNLIMITED, SCALE_UNLIMITED);

    /** VARCHAR data type. */
    public static final DataType VARCHAR = new DataType(BaseDataType.STRING, PRECISION_UNLIMITED, SCALE_UNLIMITED);

    /** DECIMAL data type created from BigInteger instance. */
    public static final DataType DECIMAL_INTEGER_BIGINT = new DataType(
        BaseDataType.BIG_INTEGER,
        PRECISION_UNLIMITED,
        0
    );

    /** DECIMAL data type with zero scale created from BigDecimal instance. */
    public static final DataType DECIMAL_INTEGER_DECIMAL = new DataType(
        BaseDataType.BIG_DECIMAL,
        PRECISION_UNLIMITED,
        0
    );

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

        if (clazz == BaseDataType.BOOLEAN.getClazz())
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
            return DECIMAL_INTEGER_BIGINT;
        else if (clazz == BaseDataType.BIG_DECIMAL.getClazz())
            return DECIMAL;
        else if (clazz == BaseDataType.FLOAT.getClazz())
            return REAL;
        else if (clazz == BaseDataType.DOUBLE.getClazz())
            return DOUBLE;
        else if (clazz == BaseDataType.STRING.getClazz())
            return VARCHAR;

        // TODO: We will have to return "OBJECT" here instead for nested field access.
        throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Unsupported data type: " + clazz.getSimpleName());
    }

    public DataType(BaseDataType baseType, int precision, int scale) {
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
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type.");
    }

    public boolean isNumeric() {
        return baseType.getGroup() == BaseDataTypeGroup.NUMERIC;
    }

    @Override
    public String toString() {
        return "DataType{base=" + baseType.name() + ", precision=" + precision + ", scale=" + scale + "}";
    }
}
