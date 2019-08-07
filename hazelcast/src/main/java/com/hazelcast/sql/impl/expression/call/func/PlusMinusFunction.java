package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.BiCallExpressionWithType;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.io.IOException;
import java.math.BigDecimal;

import static com.hazelcast.sql.impl.type.DataType.PRECISION_UNLIMITED;
import static com.hazelcast.sql.impl.type.DataType.SCALE_UNLIMITED;

/**
 * Plus expression.
 */
public class PlusMinusFunction<T> extends BiCallExpressionWithType<T> {
    /** Minus flag. */
    private boolean minus;

    /** Type of the first argument. */
    private transient DataType operand1Type;

    /** Type of the second argument. */
    private transient DataType operand2Type;

    public PlusMinusFunction() {
        // No-op.
    }

    public PlusMinusFunction(Expression operand1, Expression operand2, boolean minus) {
        super(operand1, operand2);

        this.minus = minus;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        // Calculate child operands with fail-fast NULL semantics.
        Object operand1Value = operand1.eval(ctx, row);

        if (operand1Value == null)
            return null;

        Object operand2Value = operand2.eval(ctx, row);

        if (operand2Value == null)
            return null;

        // Prepare result type if needed.
        if (resType == null) {
            DataType type1 = operand1.getType();
            DataType type2 = operand2.getType();

            resType = inferResultType(type1, type2);

            operand1Type = type1;
            operand2Type = type2;
        }

        // Execute.
        if (minus)
            return (T)doMinus(operand1Value, operand1Type, operand2Value, operand2Type, resType);
        else
            return (T)doSum(operand1Value, operand1Type, operand2Value, operand2Type, resType);
    }

    @SuppressWarnings("unchecked")
    private static Object doSum(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resType
    ) {
        Converter operand1Converter = operand1Type.getConverter();
        Converter operand2Converter = operand2Type.getConverter();

        switch (resType.getType()) {
            case TINYINT:
                return operand1Converter.asTinyInt(operand1) + operand2Converter.asTinyInt(operand2);

            case SMALLINT:
                return operand1Converter.asSmallInt(operand1) + operand2Converter.asSmallInt(operand2);

            case INT:
                return operand1Converter.asInt(operand1) + operand2Converter.asInt(operand2);

            case BIGINT:
                return operand1Converter.asBigInt(operand1) + operand2Converter.asBigInt(operand2);

            case DECIMAL:
                BigDecimal op1Decimal = operand1Converter.asDecimal(operand1);
                BigDecimal op2Decimal = operand2Converter.asDecimal(operand2);

                return op1Decimal.add(op2Decimal);

            case REAL:
                return operand1Converter.asReal(operand1) + operand2Converter.asReal(operand2);

            case DOUBLE:
                return operand1Converter.asDouble(operand1) + operand2Converter.asDouble(operand2);

            default:
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
        }
    }

    private static Object doMinus(
        Object operand1,
        DataType operand1Type,
        Object operand2,
        DataType operand2Type,
        DataType resType
    ) {
        Converter operand1Converter = operand1Type.getConverter();
        Converter operand2Converter = operand2Type.getConverter();

        switch (resType.getType()) {
            case TINYINT:
                return operand1Converter.asTinyInt(operand1) - operand2Converter.asTinyInt(operand2);

            case SMALLINT:
                return operand1Converter.asSmallInt(operand1) - operand2Converter.asSmallInt(operand2);

            case INT:
                return operand1Converter.asInt(operand1) - operand2Converter.asInt(operand2);

            case BIGINT:
                return operand1Converter.asBigInt(operand1) - operand2Converter.asBigInt(operand2);

            case DECIMAL:
                BigDecimal op1Decimal = operand1Converter.asDecimal(operand1);
                BigDecimal op2Decimal = operand2Converter.asDecimal(operand2);

                return op1Decimal.subtract(op2Decimal);

            case REAL:
                return operand1Converter.asReal(operand1) - operand2Converter.asReal(operand2);

            case DOUBLE:
                return operand1Converter.asDouble(operand1) - operand2Converter.asDouble(operand2);

            default:
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
        }
    }

    /**
     * Infer result type.
     *
     * @param type1 Type of the first operand.
     * @param type2 Type of the second operand.
     * @return Result type.
     */
    private static DataType inferResultType(DataType type1, DataType type2) {
        if (!type1.isCanConvertToNumeric())
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 1 is not numeric.");

        if (!type2.isCanConvertToNumeric())
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 2 is not numeric.");

        if (type1 == DataType.VARCHAR)
            type1 = DataType.DECIMAL;

        if (type2 == DataType.VARCHAR)
            type2 = DataType.DECIMAL;

        // Precision is expanded by 1 to handle overflow: 9 + 1 = 10
        int precision = type1.getPrecision() == PRECISION_UNLIMITED || type2.getPrecision() == PRECISION_UNLIMITED ?
            PRECISION_UNLIMITED : Math.max(type1.getPrecision(), type2.getPrecision()) + 1;

        // We have only unlimited or zero scales.
        int scale = type1.getScale() == SCALE_UNLIMITED || type2.getScale() == SCALE_UNLIMITED ? SCALE_UNLIMITED : 0;

        if (scale == 0)
            return DataType.integerType(precision);
        else {
            DataType biggerType = type1.getPrecedence() >= type2.getPrecedence() ? type1 : type2;

            if (biggerType == DataType.REAL)
                return DataType.DOUBLE; // REAL -> DOUBLE
            else
                return biggerType;      // DECIMAL -> DECIMAL, DOUBLE -> DOUBLE
        }
    }

    @Override public int operator() {
        return minus ? CallOperator.MINUS : CallOperator.PLUS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(minus);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        minus = in.readBoolean();
    }
}
