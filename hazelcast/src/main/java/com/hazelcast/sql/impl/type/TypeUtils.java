package com.hazelcast.sql.impl.type;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.accessor.Converter;

public class TypeUtils {
    /** Constant: unlimited precision. */
    public static final int PRECISION_UNLIMITED = -1;

    /** Constant: unlimited scale. */
    public static final int SCALE_UNLIMITED = -1;

    /** Scale for division. */
    public static final int SCALE_DIVIDE = 38;

    /** Precision of BOOLEAN. */
    public static final int PRECISION_BIT = 1;

    /** Precision of TINYINT. */
    public static final int PRECISION_TINYINT = 4;

    /** Precision of SMALLINT. */
    public static final int PRECISION_SMALLINT = 7;

    /** Precision of INT. */
    public static final int PRECISION_INT = 11;

    /** Precision of BIGINT */
    public static final int PRECISION_BIGINT = 20;

    /** Precedence of LATE data type. */
    public static final int PRECEDENCE_LATE = 0;

    /** Precedence of STRING data type. */
    public static final int PRECEDENCE_STRING = 1;

    /** Precedence of BOOLEAN data type. */
    public static final int PRECEDENCE_BOOLEAN = 2;

    /** Precedence of BYTE data type. */
    public static final int PRECEDENCE_BYTE = 3;

    /** Precedence of SHORT data type. */
    public static final int PRECEDENCE_SHORT = 4;

    /** Precedence of INTEGER data type. */
    public static final int PRECEDENCE_INTEGER = 5;

    /** Precedence of LONG data type. */
    public static final int PRECEDENCE_LONG = 6;

    /** Precedence of BIG_INTEGER data type. */
    public static final int PRECEDENCE_BIG_INTEGER = 7;

    /** Precedence of BIG_DECIMAL data type. */
    public static final int PRECEDENCE_BIG_DECIMAL = 8;

    /** Precedence of FLOAT data type. */
    public static final int PRECEDENCE_FLOAT = 9;

    /** Precedence of DOUBLE data type. */
    public static final int PRECEDENCE_DOUBLE = 10;

    /** Precedence of temporal data types. */
    public static final int PRECEDENCE_TIMESTAMP = 11;

    /** Common cached integer data types. */
    private static DataType[] INTEGER_TYPES = new DataType[PRECISION_BIGINT];

    static {
        for (int i = 1; i < PRECISION_BIGINT; i++) {
            DataType type;

            if (i == PRECISION_BIT)
                type = DataType.BIT;
            else if (i < PRECISION_TINYINT)
                type = new DataType(BaseDataType.BYTE, i, 0);
            else if (i == PRECISION_TINYINT)
                type = DataType.TINYINT;
            else if (i < PRECISION_SMALLINT)
                type = new DataType(BaseDataType.SHORT, i, 0);
            else if (i == PRECISION_SMALLINT)
                type = DataType.SMALLINT;
            else if (i < PRECISION_INT)
                type = new DataType(BaseDataType.INTEGER, i, 0);
            else if (i == PRECISION_INT)
                type = DataType.INT;
            else
                type = new DataType(BaseDataType.LONG, i, 0);

            INTEGER_TYPES[i] = type;
        }
    }

    /**
     * Make sure that the type is numeric.
     *
     * @param type Type.
     */
    private static void ensureNumeric(DataType type) {
        if (!type.isNumeric() && type.getBaseType() != BaseDataType.STRING)
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 1 is not numeric.");
    }

    /**
     * Make sure that the type is numeric.
     *
     * @param type1 Type 1.
     * @param type2 Type 2.
     */
    private static void ensureNumeric(DataType type1, DataType type2) {
        if (!type1.isNumeric() && type1.getBaseType() != BaseDataType.STRING)
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 1 is not numeric.");

        if (!type2.isNumeric() && type2.getBaseType() != BaseDataType.STRING)
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 2 is not numeric.");
    }

    public static Converter numericAccessor(Expression expr) {
        DataType type = expr.getType();

        if (!type.isNumeric() && type.getBaseType() != BaseDataType.STRING)
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand is not numeric: " + type);

        return type.getBaseType().getAccessor();
    }

    public static Converter numericAccessor(Expression expr, int operandPos) {
        DataType type = expr.getType();

        if (!type.isNumeric() && type.getBaseType() != BaseDataType.STRING)
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand " + operandPos + " is not numeric: " + type);

        return type.getBaseType().getAccessor();
    }

    /**
     * Infer result type for unary minus operation.
     *
     * @param type Type.
     * @return Result type.
     */
    public static DataType inferForUnaryMinus(DataType type) {
        ensureNumeric(type);

        if (type.getBaseType() == BaseDataType.STRING)
            type = DataType.DECIMAL;

        if (type.getScale() == 0) {
            // Integer type.
            int precision = type.getPrecision();

            if (precision != PRECISION_UNLIMITED)
                precision++;

            return integerType(precision);
        }
        else {
            // DECIMAL, FLOAT or DOUBLE. FLOAT is expanded to DOUBLE. DECIMAL and DOUBLE are already the widest.
            return type.getBaseType() == BaseDataType.FLOAT ? DataType.DOUBLE : type;
        }
    }

    /**
     * Infer result type for plus or minus operation.
     *
     * @param type1 Type 1.
     * @param type2 Type 2.
     * @return Result type.
     */
    public static DataType inferForPlusMinus(DataType type1, DataType type2) {
        ensureNumeric(type1, type2);

        if (type1.getBaseType() == BaseDataType.STRING)
            type1 = DataType.DECIMAL;

        if (type2.getBaseType() == BaseDataType.STRING)
            type2 = DataType.DECIMAL;

        // Precision is expanded by 1 to handle overflow: 9 + 1 = 10
        int precision = type1.getPrecision() == PRECISION_UNLIMITED || type2.getPrecision() == PRECISION_UNLIMITED ?
            PRECISION_UNLIMITED : Math.max(type1.getPrecision(), type2.getPrecision()) + 1;

        // We have only unlimited or zero scales.
        int scale = type1.getScale() == SCALE_UNLIMITED || type2.getScale() == SCALE_UNLIMITED ? SCALE_UNLIMITED : 0;

        if (scale == 0)
            return integerType(precision);
        else {
            DataType biggerType = type1.getPrecedence() >= type2.getPrecedence() ? type1 : type2;

            BaseDataType baseType = biggerType.getBaseType();

            if (baseType == BaseDataType.FLOAT)
                return DataType.DOUBLE; // REAL -> DOUBLE
            else
                return biggerType; // DECIMAL -> DECIMAL, DOUBLE -> DOUBLE
        }
    }

    /**
     * Infer result type for multiplication operation.
     *
     * @param type1 Type 1.
     * @param type2 Type 2.
     * @return Result type.
     */
    public static DataType inferForMultiply(DataType type1, DataType type2) {
        ensureNumeric(type1, type2);

        if (type1.getBaseType() == BaseDataType.STRING)
            type1 = DataType.DECIMAL;

        if (type2.getBaseType() == BaseDataType.STRING)
            type2 = DataType.DECIMAL;

        // Precision is expanded to accommodate all numbers: 99 * 99 = 9801;
        int precision = type1.getPrecision() == PRECISION_UNLIMITED || type2.getPrecision() == PRECISION_UNLIMITED ?
            PRECISION_UNLIMITED : type1.getPrecision() + type2.getPrecision();

        int scale = type1.getScale() == SCALE_UNLIMITED || type2.getScale() == SCALE_UNLIMITED ? SCALE_UNLIMITED : 0;

        if (scale == 0)
            return integerType(precision);
        else {
            DataType biggerType = type1.getPrecedence() >= type2.getPrecedence() ? type1 : type2;

            BaseDataType baseType = biggerType.getBaseType();

            if (baseType == BaseDataType.FLOAT)
                return DataType.DOUBLE; // REAL -> DOUBLE
            else
                return biggerType;      // DECIMAL -> DECIMAL, DOUBLE -> DOUBLE
        }
    }

    /**
     * Infer result type for division.
     *
     * @param type1 Type 1.
     * @param type2 Type 2.
     * @return Result type.
     */
    public static DataType inferForDivideRemainder(DataType type1, DataType type2) {
        ensureNumeric(type1, type2);

        if (type1.getBaseType() == BaseDataType.STRING)
            type1 = DataType.DECIMAL;

        if (type2.getBaseType() == BaseDataType.STRING)
            type2 = DataType.DECIMAL;

        if (type1.getBaseType() == BaseDataType.BOOLEAN)
            throw new HazelcastSqlException(-1, "Boolean operand cannot be used as dividend: " + type1);

        if (type2.getBaseType() == BaseDataType.BOOLEAN)
            throw new HazelcastSqlException(-1, "Boolean operand cannot be used as divisor: " + type2);

        DataType higherType = type1.getPrecedence() > type2.getPrecedence() ? type1 : type2;

        switch (higherType.getBaseType()) {
            case BYTE:
                return DataType.TINYINT;

            case SHORT:
                return DataType.SMALLINT;

            case INTEGER:
                return DataType.INT;

            case LONG:
                return DataType.BIGINT;

            case FLOAT:
                return DataType.REAL;

            case DOUBLE:
                return DataType.DOUBLE;

            default:
                return DataType.DECIMAL;
        }
    }

    /**
     * Get integer type for the given precision.
     *
     * @param precision Precision.
     * @return Type.
     */
    private static DataType integerType(int precision) {
        assert precision != 0;

        if (precision == PRECISION_UNLIMITED)
            return DataType.DECIMAL_INTEGER_DECIMAL;
        else if (precision < PRECISION_BIGINT)
            return INTEGER_TYPES[precision];
        else
            return new DataType(BaseDataType.BIG_DECIMAL, precision, 0);
    }

    /**
     * Return passed data type or {@link DataType#LATE} if the argument is {@code null}.
     *
     * @param type Type.
     * @return Same type or {@link DataType#LATE}.
     */
    public static DataType notNullOrLate(DataType type) {
        return type != null ? type : DataType.LATE;
    }

    private TypeUtils() {
        // No-op.
    }
}
