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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Objects;

import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;

@NotThreadSafe
public class AvgSqlAggregation extends SqlAggregation {

    private QueryDataType resultType;

    private SumSqlAggregation sum;
    private CountSqlAggregation count;

    @SuppressWarnings("unused")
    private AvgSqlAggregation() {
    }

    public AvgSqlAggregation(int index, QueryDataType operandType) {
        this(index, operandType, false);
    }

    public AvgSqlAggregation(int index, QueryDataType operandType, boolean distinct) {
        super(index, true, distinct);
        this.resultType = inferResultType(operandType);
        this.sum = new SumSqlAggregation(index, operandType);
        this.count = new CountSqlAggregation();
    }

    private static QueryDataType inferResultType(QueryDataType operandType) {
        switch (operandType.getTypeFamily()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
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
        sum.accumulate(value);
        count.accumulate(value);
    }

    @Override
    public void combine(SqlAggregation other0) {
        AvgSqlAggregation other = (AvgSqlAggregation) other0;

        sum.combine(other.sum);
        count.combine(other.count);
    }

    @Override
    public Object collect() {
        Object sum = this.sum.collect();
        if (sum == null) {
            return null;
        }
        Object count = this.count.collect();

        switch (resultType.getTypeFamily()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
                BigDecimal decimalSum = this.sum.resultType().getConverter().asDecimal(sum);
                BigDecimal decimalCount = this.count.resultType().getConverter().asDecimal(count);
                return decimalSum.divide(decimalCount, DECIMAL_MATH_CONTEXT);
            default:
                assert resultType.getTypeFamily() == QueryDataTypeFamily.DOUBLE;
                double doubleSum = this.sum.resultType().getConverter().asDouble(sum);
                double doubleCount = this.count.resultType().getConverter().asDouble(count);
                return doubleSum / doubleCount;
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(resultType);
        out.writeObject(sum);
        out.writeObject(count);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        resultType = in.readObject();
        sum = in.readObject();
        count = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AvgSqlAggregation that = (AvgSqlAggregation) o;
        return Objects.equals(resultType, that.resultType) &&
                Objects.equals(sum, that.sum) &&
                Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultType, sum, count);
    }
}
