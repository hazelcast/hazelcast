package com.hazelcast.sql.impl.type;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.accessor.BaseDataTypeAccessor;

import java.util.function.BiFunction;

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

    /** Precedence of BOOLEAN data type. */
    public static final int PRECEDENCE_BOOLEAN = 1;

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
        if (!type.isNumeric())
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 1 is not numeric.");
    }

    /**
     * Make sure that the type is numeric.
     *
     * @param type1 Type 1.
     * @param type2 Type 2.
     */
    private static void ensureNumeric(DataType type1, DataType type2) {
        if (!type1.isNumeric())
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 1 is not numeric.");

        if (!type2.isNumeric())
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 2 is not numeric.");
    }

    public static BaseDataTypeAccessor numericAccessor(Expression expr) {
        DataType type = expr.getType();

        if (!type.isNumeric())
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand is not numeric: " + type);

        return type.getBaseType().getAccessor();
    }

    public static BaseDataTypeAccessor numericAccessor(Expression expr, int operandPos) {
        DataType type = expr.getType();

        if (!type.isNumeric())
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

        // Precision is expanded by 1 to handle overflow: 9 + 1 = 10
        int precision = calculatePrecision(
            type1.getPrecision(),
            type2.getPrecision(),
            false,
            (p1, p2) -> Math.max(p1, p2) + 1);

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

        // Precision is expanded to accomodate all numbers: 99 * 99 = 9801;
        int precision = calculatePrecision(
            type1.getPrecision(),
            type2.getPrecision(),
            false,
            (p1, p2) -> p1 + p2);

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

    private static int calculatePrecision(
        int precision1,
        int precision2,
        boolean ignoreUnlimited,
        BiFunction<Integer, Integer, Integer> func
    ) {
        if (!ignoreUnlimited && (precision1 == PRECISION_UNLIMITED || precision2 == PRECISION_UNLIMITED))
            return PRECISION_UNLIMITED;

        return func.apply(precision1, precision2);
    }

    private static int calculateScale(
        int scale1,
        int scale2,
        boolean ignoreUnlimited,
        BiFunction<Integer, Integer, Integer> func
    ) {
        if (!ignoreUnlimited && (scale1 == SCALE_UNLIMITED || scale2 == SCALE_UNLIMITED))
            return SCALE_UNLIMITED;

        return func.apply(scale1, scale2);
    }

    public static DataType notNull(DataType type) {
        return type != null ? type : DataType.LATE;
    }

    private TypeUtils() {
        // No-op.
    }
}
