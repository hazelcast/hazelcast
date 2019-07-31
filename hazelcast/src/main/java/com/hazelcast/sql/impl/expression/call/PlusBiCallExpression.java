package com.hazelcast.sql.impl.expression.call;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.BaseDataType;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.BaseDataTypeAccessor;

import java.math.BigDecimal;

/**
 * Plus expression.
 */
public class PlusBiCallExpression<T> extends BiCallExpression<T> {
    /** Result type. */
    private transient DataType resType;

    /** Accessor for the first argument. */
    private transient BaseDataTypeAccessor accessor1;

    /** Accessor for the second argument. */
    private transient BaseDataTypeAccessor accessor2;

    public PlusBiCallExpression() {
        // No-op.
    }

    public PlusBiCallExpression(Expression operand1, Expression operand2) {
        super(operand1, operand2);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        // Calculate child operands with fail-fast NULL semantics.
        Object res1 = operand1.eval(ctx, row);

        if (res1 == null)
            return null;

        Object res2 = operand2.eval(ctx, row);

        if (res2 == null)
            return null;

        // Prepare result type if needed.
        if (resType == null)
            prepare();

        return (T)eval0(res1, res2);
    }

    /**
     * Prepare result type. Occurs once per operator.
     */
    private void prepare() {
        // Check if numeric.
        DataType type1 = operand1.getType();

        if (!type1.isNumeric())
            throw new HazelcastSqlException(-1, "Operand 1 is not numeric.");

        DataType type2 = operand2.getType();

        if (!type2.isNumeric())
            throw new HazelcastSqlException(-1, "Operand 2 is not numeric.");

        accessor1 = type1.getBaseType().getAccessor();
        accessor2 = type2.getBaseType().getAccessor();

        // Expand.
        int precision = type1.getPrecision() == DataType.PRECISION_UNLIMITED || type1.getPrecision() == DataType.PRECISION_UNLIMITED ?
            DataType.PRECISION_UNLIMITED : Math.max(type1.getPrecision(), type2.getPrecision()) + 1;

        int scale = type1.getScale() == DataType.SCALE_UNLIMITED || type2.getScale() == DataType.SCALE_UNLIMITED ?
            DataType.SCALE_UNLIMITED : Math.max(type1.getScale(), type2.getScale());

        if (scale == 0) {
            // Just integer result.
            resType = DataType.integerType(precision);
        }
        else {
            assert scale == DataType.SCALE_UNLIMITED;

            DataType biggerType = type1.getPrecedence() >= type2.getPrecedence() ? type1 : type2;

            BaseDataType baseType = biggerType.getBaseType();

            if (baseType == BaseDataType.FLOAT) {
                // REAL -> DOUBLE
                resType = DataType.DOUBLE;
            }
            else
                // DECIMAL -> DECIMAL, DOUBLE -> DOUBLE
                resType = biggerType;
        }
    }

    @SuppressWarnings("unchecked")
    private Object eval0(Object op1, Object op2) {
        switch (resType.getBaseType()) {
            case BYTE:
                return accessor1.getByte(op1) + accessor2.getByte(op2);

            case SHORT:
                return ((Number)op1).shortValue() + ((Number)op2).shortValue();

            case INTEGER:
                return ((Number)op1).intValue() + ((Number)op2).intValue();

            case LONG:
                return ((Number)op1).longValue() + ((Number)op2).longValue();

            case BIG_DECIMAL:
                BigDecimal op1Decimal = accessor1.getDecimal(op1);
                BigDecimal op2Decimal = accessor2.getDecimal(op2);

                return op1Decimal.add(op2Decimal);

            case FLOAT:
                return ((Number)op1).floatValue() + ((Number)op2).floatValue();

            case DOUBLE:
                return ((Number)op1).doubleValue() + ((Number)op2).doubleValue();

            default:
                // TODO: Proper exception.
                throw new IllegalStateException("Invalid type: " + resType);
        }
    }

    @Override
    public DataType getType() {
        return resType;
    }

    @Override public int operator() {
        return CallOperator.PLUS;
    }
}
