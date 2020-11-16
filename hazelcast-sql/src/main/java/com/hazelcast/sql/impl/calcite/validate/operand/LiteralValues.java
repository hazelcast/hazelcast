package com.hazelcast.sql.impl.calcite.validate.operand;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import org.apache.calcite.sql.SqlLiteral;

import java.math.BigDecimal;
import java.util.Locale;

public final class LiteralValues {

    public static final LiteralValues NULL = new LiteralValues(null, null, null, null, null, true);
    public static final LiteralValues BOOLEAN_TRUE = new LiteralValues(null, LiteralValue.BOOLEAN_TRUE, null, null, null, false);
    public static final LiteralValues BOOLEAN_FALSE = new LiteralValues(null, LiteralValue.BOOLEAN_FALSE, null, null, null, false);

    private static final String DOUBLE_POSITIVE_INFINITY = "Infinity";
    private static final String DOUBLE_NEGATIVE_INFINITY = "-Infinity";
    private static final String DOUBLE_NAN = "NaN";
    private static final String DOUBLE_EXPONENT = "e";

    private final LiteralValue stringValue;
    private final LiteralValue booleanValue;
    private final LiteralValue bigintValue;
    private final LiteralValue decimalValue;
    private final LiteralValue doubleValue;
    private final boolean isNull;

    public LiteralValues(
        LiteralValue stringValue,
        LiteralValue booleanValue,
        LiteralValue bigintValue,
        LiteralValue decimalValue,
        LiteralValue doubleValue,
        boolean isNull
    ) {
        this.stringValue = stringValue;
        this.booleanValue = booleanValue;
        this.bigintValue = bigintValue;
        this.decimalValue = decimalValue;
        this.doubleValue = doubleValue;
        this.isNull = isNull;
    }

    public boolean isNull() {
        return isNull;
    }

    public LiteralValue asVarchar() {
        // TODO
        return null;
    }

    public static LiteralValues parse(SqlLiteral literal) {
        if (literal.getValue() == null) {
            return null;
        }

        switch (literal.getTypeName()) {
            case CHAR:
            case VARCHAR:
                return parseString(literal);

            case BOOLEAN:
                return parseBoolean(literal);

            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return parseBigint(literal);

            case DECIMAL:
                return parseDecimal(literal);

            case FLOAT:
            case REAL:
            case DOUBLE:
                return parseDouble(literal);

            case NULL:
                return NULL;

            default:
                return null;
        }
    }

    private static LiteralValues parseString(SqlLiteral literal) {
        String value = literal.getValueAs(String.class);
        LiteralValue stringValue = new LiteralValue(QueryDataType.VARCHAR, value);

        LiteralValue booleanValue = null;
        LiteralValue bigintValue = null;
        LiteralValue decimalValue = null;
        LiteralValue doubleValue = null;

        String lowerValue = value.toLowerCase(Locale.ROOT);

        boolean continueParse = true;

        // Boolean?
        if (lowerValue.equals(Boolean.TRUE.toString())) {
            booleanValue = LiteralValue.BOOLEAN_TRUE;
            continueParse = false;
        } else if (lowerValue.equals(Boolean.FALSE.toString())) {
            booleanValue = LiteralValue.BOOLEAN_FALSE;
            continueParse = false;
        }

        // Integer?
        if (continueParse) {
            try {
                bigintValue = new LiteralValue(QueryDataType.BIGINT, Long.parseLong(lowerValue));
                continueParse = false;
            } catch (NumberFormatException ignore) {
                // No-op
            }
        }

        // Fractional, inexact?
        if (continueParse) {
            Double doubleValue0 = null;

            if (DOUBLE_POSITIVE_INFINITY.equals(value)) {
                doubleValue0 = Double.POSITIVE_INFINITY;
            } else if (DOUBLE_NEGATIVE_INFINITY.equals(value)) {
                doubleValue0 = Double.NEGATIVE_INFINITY;
            } else if (DOUBLE_NAN.equals(value)) {
                doubleValue0 = Double.NaN;
            } else if (lowerValue.contains(DOUBLE_EXPONENT)) {
                try {
                    doubleValue0 = StringConverter.INSTANCE.asDouble(value);
                } catch (Exception ignore) {
                    // No-op.
                }
            }

            if (doubleValue0 != null) {
                doubleValue = new LiteralValue(QueryDataType.DOUBLE, doubleValue0);
                continueParse = false;
            }
        }

        // Fractional, exact?
        if (continueParse) {
            try {
                decimalValue = new LiteralValue(QueryDataType.DECIMAL, new BigDecimal(lowerValue));
            } catch (Exception ignore) {
                // No-op.
            }
        }

        return new LiteralValues(
            stringValue,
            booleanValue,
            bigintValue,
            decimalValue,
            doubleValue,
            false
        );
    }

    private static LiteralValues parseBoolean(SqlLiteral literal) {
        // BOOLEAN literal represents only self
        return literal.booleanValue() ? LiteralValues.BOOLEAN_TRUE : LiteralValues.BOOLEAN_FALSE;
    }

    private static LiteralValues parseBigint(SqlLiteral literal) {
        // BIGINT literal represents only self
        return new LiteralValues(
            null,
            null,
            new LiteralValue(QueryDataType.BIGINT, literal.getValueAs(Long.class)),
            null,
            null,
            false
        );
    }

    private static LiteralValues parseDecimal(SqlLiteral literal) {
        // DECIMAL literal represents only self
        return new LiteralValues(
            null,
            null,
            null,
            new LiteralValue(QueryDataType.DECIMAL, literal.getValueAs(BigDecimal.class)),
            null,
            false
        );
    }

    private static LiteralValues parseDouble(SqlLiteral literal) {
        // DOUBLE literal represents only self
        return new LiteralValues(
            null,
            null,
            null,
            null,
            new LiteralValue(QueryDataType.DOUBLE, literal.getValueAs(Double.class)),
            false
        );
    }
}
