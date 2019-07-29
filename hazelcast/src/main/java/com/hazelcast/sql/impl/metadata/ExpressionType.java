package com.hazelcast.sql.impl.metadata;

import java.math.BigDecimal;

public class ExpressionType {

    public static final int PRECEDENCE_TINYINT = 1;
    public static final int PRECEDENCE_SMALLINT = 2;
    public static final int PRECEDENCE_INT = 3;
    public static final int PRECEDENCE_BIGINT = 4;
    public static final int PRECEDENCE_DECIMAL = 5;
    public static final int PRECEDENCE_REAL = 6;
    public static final int PRECEDENCE_DOUBLE = 7;

    public static final ExpressionType UNKNOWN = new ExpressionType(false, -1, null);

    public static final ExpressionType TINYINT = new ExpressionType(true, PRECEDENCE_TINYINT, Byte.class);
    public static final ExpressionType SMALLINT = new ExpressionType(true, PRECEDENCE_SMALLINT, Short.class);
    public static final ExpressionType INT = new ExpressionType(true, PRECEDENCE_INT, Integer.class);
    public static final ExpressionType BIGINT = new ExpressionType(true, PRECEDENCE_BIGINT, Long.class);
    public static final ExpressionType DECIMAL = new ExpressionType(true, PRECEDENCE_DECIMAL, BigDecimal.class);
    public static final ExpressionType REAL = new ExpressionType(true, PRECEDENCE_REAL, Float.class);
    public static final ExpressionType DOUBLE = new ExpressionType(true, PRECEDENCE_DOUBLE, Double.class);

    private final boolean numeric;
    private final int precedence;
    private final Class clazz;

    private ExpressionType(boolean numeric, int precedence, Class clazz) {
        this.numeric = numeric;
        this.precedence = precedence;
        this.clazz = clazz;
    }

    public boolean isNumeric() {
        return numeric;
    }

    public int precedence() {
        return precedence;
    }

    /**
     * @param candidate Object to check.
     * @return {@code True} if the given object is of the given type.
     */
    public boolean isSame(Object candidate) {
        // All SQL types are nullable, so null instance is always of expected type.
        if (candidate == null)
            return true;

        return clazz == candidate.getClass();
    }

    public static ExpressionType of(Object obj) {
        if (obj == null)
            return null;
        else {
            if (obj instanceof Byte)
                return ExpressionType.TINYINT;
            else if (obj instanceof Short)
                return ExpressionType.SMALLINT;
            else if (obj instanceof Integer)
                return ExpressionType.INT;
            else if (obj instanceof Long)
                return ExpressionType.BIGINT;
            else if (obj instanceof BigDecimal)
                return ExpressionType.DECIMAL;
            else if (obj instanceof Float)
                return ExpressionType.REAL;
            else if (obj instanceof Double)
                return ExpressionType.DOUBLE;
            else
                // TODO: Proper exception.
                throw new IllegalArgumentException("Unsupoprted data type: " + obj);
        }
    }
}
