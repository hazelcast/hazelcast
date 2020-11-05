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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converter;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Objects;

import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;

@NotThreadSafe
public class SumSqlAggregation extends SqlAggregation {

    private QueryDataType operandType;
    private QueryDataType resultType;

    private Object value;

    @SuppressWarnings("unused")
    private SumSqlAggregation() {
    }

    public SumSqlAggregation(int index, QueryDataType operandType) {
        this(index, operandType, false);
    }

    public SumSqlAggregation(int index, QueryDataType operandType, boolean distinct) {
        super(index, true, distinct);
        this.operandType = operandType;
        this.resultType = inferResultType(operandType);
    }

    private static QueryDataType inferResultType(QueryDataType operandType) {
        switch (operandType.getTypeFamily()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return QueryDataType.BIGINT;
            case DECIMAL:
                return QueryDataType.DECIMAL;
            case REAL:
            case DOUBLE:
                return QueryDataType.DOUBLE;
            default:
                throw QueryException.error("Unsupported operand type: " + operandType);
        }
    }

    @Override
    public QueryDataType resultType() {
        return resultType;
    }

    @Override
    protected void accumulate(Object value) {
        add(value, operandType.getConverter());
    }

    @Override
    public void combine(SqlAggregation other0) {
        SumSqlAggregation other = (SumSqlAggregation) other0;

        Object value = other.value;
        if (value != null) {
            add(value, resultType.getConverter());
        }
    }

    private void add(Object value, Converter converter) {
        if (this.value == null) {
            this.value = identity();
        }

        switch (resultType.getTypeFamily()) {
            case BIGINT:
                try {
                    this.value = Math.addExact((long) this.value, converter.asBigint(value));
                } catch (ArithmeticException e) {
                    throw QueryException.dataException(QueryDataTypeFamily.BIGINT + " overflow in 'SUM' function " +
                            "(consider adding explicit CAST to DECIMAL)");
                }
                break;
            case DECIMAL:
                this.value = ((BigDecimal) this.value).add(converter.asDecimal(value), DECIMAL_MATH_CONTEXT);
                break;
            default:
                assert resultType.getTypeFamily() == QueryDataTypeFamily.DOUBLE;
                this.value = (double) this.value + converter.asDouble(value);
        }
    }

    private Object identity() {
        switch (resultType.getTypeFamily()) {
            case BIGINT:
                return 0L;
            case DECIMAL:
                return BigDecimal.ZERO;
            default:
                assert resultType.getTypeFamily() == QueryDataTypeFamily.DOUBLE;
                return 0.0D;
        }
    }

    @Override
    public Object collect() {
        return value;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(operandType);
        out.writeObject(resultType);
        out.writeObject(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        operandType = in.readObject();
        resultType = in.readObject();
        value = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SumSqlAggregation that = (SumSqlAggregation) o;
        return Objects.equals(operandType, that.operandType) &&
                Objects.equals(resultType, that.resultType) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operandType, resultType, value);
    }
}
