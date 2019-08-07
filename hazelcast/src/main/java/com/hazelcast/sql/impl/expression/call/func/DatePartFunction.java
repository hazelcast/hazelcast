package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.TemporalUtils;
import com.hazelcast.sql.impl.expression.TemporalValue;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.temporal.WeekFields;

/**
 * Family of date part functions.
 */
public class DatePartFunction extends UniCallExpression<Integer> {
    /** Unit. */
    private DatePartUnit unit;

    /** Operand type. */
    private transient DataType operandType;

    public DatePartFunction() {
        // No-op.
    }

    public DatePartFunction(Expression operand, DatePartUnit unit) {
        super(operand);

        this.unit = unit;
    }

    @Override
    public Integer eval(QueryContext ctx, Row row) {
        Object operandValue = operand.eval(ctx, row);

        if (operandValue == null)
            return null;

        if (operandType == null) {
            DataType type = operand.getType();

            switch (type.getType()) {
                case VARCHAR:
                case DATE:
                case TIME:
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIMEZONE:
                    break;

                default:
                    throw new HazelcastSqlException(-1, "Type cannot be cast to DATE/TIME: " + type);
            }

            operandType = type;
        }

        return doDatePart(unit, operandValue, operandType);
    }

    private static int doDatePart(DatePartUnit unit, Object operandValue, DataType operandType) {
        OffsetDateTime dateTime;

        if (operandType == DataType.VARCHAR) {
            String stringOperandValue = operandType.getConverter().asVarchar(operandValue);

            TemporalValue parsedOperandValue = TemporalUtils.parseAny(stringOperandValue);

            Converter converter = parsedOperandValue.getType().getConverter();

            dateTime = converter.asTimestampWithTimezone(parsedOperandValue.getValue());
        }
        else
            dateTime = operandType.getConverter().asTimestampWithTimezone(operandValue);

        switch (unit) {
            case YEAR:
                return dateTime.getYear();

            case QUARTER:
                int month = dateTime.getMonthValue();

                return month <= 3 ? 1 : month <= 6 ? 2 : month <= 9 ? 3 : 4;

            case MONTH:
                return dateTime.getMonthValue();

            case WEEK:
                return dateTime.get(WeekFields.ISO.weekOfWeekBasedYear());

            case DAYOFYEAR:
                return dateTime.getDayOfYear();

            case DAYOFMONTH:
                return dateTime.getDayOfMonth();

            case DAYOFWEEK:
                return dateTime.getDayOfWeek().getValue();

            case HOUR:
                return dateTime.getHour();

            case MINUTE:
                return dateTime.getMinute();

            case SECOND:
                return dateTime.getSecond();
        }

        throw new HazelcastSqlException(-1, "Unsupported unit: " + unit);
    }

    @Override
    public int operator() {
        return CallOperator.EXTRACT;
    }

    @Override
    public DataType getType() {
        return DataType.INT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeObject(unit);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        unit = in.readObject();
    }
}
