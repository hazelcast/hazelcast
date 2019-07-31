package com.hazelcast.sql.impl.expression.call;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.BaseDataType;
import com.hazelcast.sql.impl.type.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Plus expression.
 */
public class PlusBiCallExpression<T> extends BiCallExpression<T> {
    /** Result type. */
    private transient DataType resType;

    /** Execution mode. */
    private transient Mode mode;

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

        mode = prepareMode(resType, type1, type2);
    }

    private Mode prepareMode(DataType resType, DataType type1, DataType type2) {
        switch (resType.getPrecedence()) {
            case BaseDataType.PRECEDENCE_BYTE:
                return Mode.BYTE;

            case BaseDataType.PRECEDENCE_SHORT:
                return Mode.SHORT;

            case BaseDataType.PRECEDENCE_INTEGER:
                return Mode.INT;

            case BaseDataType.PRECEDENCE_LONG:
                return Mode.LONG;

            case BaseDataType.PRECEDENCE_FLOAT:
                return Mode.FLOAT;

            case BaseDataType.PRECEDENCE_DOUBLE:
                return Mode.DOUBLE;

            case BaseDataType.PRECEDENCE_BIG_DECIMAL: {
                int precedence1 = type1.getBaseType().getPrecedence();
                int precedence2 = type2.getBaseType().getPrecedence();

                switch (precedence1) {
                    case BaseDataType.PRECEDENCE_BIG_DECIMAL:
                        switch (precedence2) {
                            case BaseDataType.PRECEDENCE_BIG_DECIMAL:
                                return Mode.DECIMAL_BD_BD;

                            case BaseDataType.PRECEDENCE_BIG_INTEGER:
                                return Mode.DECIMAL_BD_BI;

                            default:
                                return Mode.DECIMAL_BD_L;
                        }

                    case BaseDataType.PRECEDENCE_BIG_INTEGER: {
                        switch (precedence2) {
                            case BaseDataType.PRECEDENCE_BIG_DECIMAL:
                                return Mode.DECIMAL_BI_BD;

                            case BaseDataType.PRECEDENCE_BIG_INTEGER:
                                return Mode.DECIMAL_BI_BI;

                            default:
                                return Mode.DECIMAL_BI_L;
                        }
                    }

                    default: {
                        switch (precedence2) {
                            case BaseDataType.PRECEDENCE_BIG_DECIMAL:
                                return Mode.DECIMAL_L_BD;

                            case BaseDataType.PRECEDENCE_BIG_INTEGER:
                                return Mode.DECIMAL_L_BI;

                            default:
                                return Mode.DECIMAL_L_L;
                        }
                    }
                }
            }

            default:
                // TODO: Proper exception.
                throw new IllegalStateException("Invalid precedence: " + resType.getPrecedence());
        }
    }

    @SuppressWarnings("unchecked")
    private Object eval0(Object op1, Object op2) {
        switch (mode) {
            case BYTE:
                if (op1 instanceof Boolean)
                    op1 = ((Boolean)op1) ? 1 : 0;

                if (op2 instanceof Boolean)
                    op2 = ((Boolean)op2) ? 1 : 0;

                return ((Number)op1).byteValue() + ((Number)op2).byteValue();

            case SHORT:
                return ((Number)op1).shortValue() + ((Number)op2).shortValue();

            case INT:
                return ((Number)op1).intValue() + ((Number)op2).intValue();

            case LONG:
                return ((Number)op1).longValue() + ((Number)op2).longValue();

            case FLOAT:
                return ((Number)op1).floatValue() + ((Number)op2).floatValue();

            case DOUBLE:
                return ((Number)op1).doubleValue() + ((Number)op2).doubleValue();

            default:
                return evalDecimal(mode, op1, op2);
        }
    }

    private Object evalDecimal(Mode mode, Object op1, Object op2) {
        switch (mode) {
            case DECIMAL_L_L:
                return BigDecimal.valueOf((long)op1).add(BigDecimal.valueOf((long)op2));

            case DECIMAL_L_BI:
                return BigDecimal.valueOf((long)op1).add(new BigDecimal((BigInteger)op2));

            case DECIMAL_L_BD:
                return BigDecimal.valueOf((long)op1).add((BigDecimal)op2);

            case DECIMAL_BI_L:
                return new BigDecimal((BigInteger)op1).add(BigDecimal.valueOf((long)op2));

            case DECIMAL_BI_BI:
                return new BigDecimal((BigInteger)op1).add(new BigDecimal((BigInteger)op2));

            case DECIMAL_BI_BD:
                return new BigDecimal((BigInteger)op1).add((BigDecimal)op2);

            case DECIMAL_BD_L:
                return ((BigDecimal)op1).add(BigDecimal.valueOf((long)op2));

            case DECIMAL_BD_BI:
                return ((BigDecimal)op1).add(new BigDecimal((BigInteger)op2));

            case DECIMAL_BD_BD:
                return ((BigDecimal)op1).add(((BigDecimal)op2));

            default:
                // TODO: Proper exception message.
                throw new IllegalArgumentException("Invalid mode: " + mode);
        }
    }

    @Override
    public DataType getType() {
        return resType;
    }

    @Override public int operator() {
        return CallOperator.PLUS;
    }

    private enum Mode {
        BYTE,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        DECIMAL_L_L,
        DECIMAL_L_BI,
        DECIMAL_L_BD,
        DECIMAL_BI_L,
        DECIMAL_BI_BI,
        DECIMAL_BI_BD,
        DECIMAL_BD_L,
        DECIMAL_BD_BI,
        DECIMAL_BD_BD
    }
}
