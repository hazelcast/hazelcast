/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.util.EnsureConvertible;
import com.hazelcast.sql.impl.expression.util.Eval;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.UniExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * Family of date part functions.
 */
public class DatePartFunction extends UniExpression<Integer> {
    /** Unit. */
    private DatePartUnit unit;

    public DatePartFunction() {
        // No-op.
    }

    private DatePartFunction(Expression<?> operand, DatePartUnit unit) {
        super(operand);

        this.unit = unit;
    }

    public static DatePartFunction create(Expression<?> operand, DatePartUnit unit) {
        EnsureConvertible.toTimestampWithTimezone(operand);

        return new DatePartFunction(operand, unit);
    }

    @Override
    public Integer eval(Row row, ExpressionEvalContext context) {
        OffsetDateTime dateTime;

        if (operand.getType() == QueryDataType.VARCHAR) {
            dateTime = DateTimeExpressionUtils.parseDateTime(Eval.asVarchar(operand, row, context));
        } else {
            dateTime = Eval.asTimestampWithTimezone(operand, row, context);
        }

        return DateTimeExpressionUtils.getDatePart(dateTime, unit);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.INT;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        DatePartFunction that = (DatePartFunction) o;

        return Objects.equals(operand, that.operand) && unit == that.unit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operand, unit);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{operand=" + operand + ", unit=" + unit + '}';
    }
}
