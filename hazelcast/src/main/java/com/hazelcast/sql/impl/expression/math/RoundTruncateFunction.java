/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.BiCallExpressionWithType;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Implementation of ROUND/TRUNCATE functions.
 */
public class RoundTruncateFunction<T> extends BiCallExpressionWithType<T> {
    /** Truncate function. */
    private boolean truncate;

    /** Value data type. */
    private transient DataType valType;

    /** Length data type. */
    private transient DataType lenType;

    public RoundTruncateFunction() {
        // No-op.
    }

    public RoundTruncateFunction(Expression operand1, Expression operand2, boolean truncate) {
        super(operand1, operand2);

        this.truncate = truncate;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        Object val = getValue(ctx, row);

        if (val == null) {
            return null;
        }

        int len = getLength(ctx, row);

        return (T) (doRoundTruncate(val, len));
    }

    /**
     * Get value.
     *
     * @param ctx Context.
     * @param row Row.
     * @return Value.
     */
    public Object getValue(QueryContext ctx, Row row) {
        Object val = operand1.eval(ctx, row);

        if (val == null) {
            return null;
        }

        if (resType == null) {
            DataType type = operand1.getType();

            switch (type.getType()) {
                case TINYINT:
                case SMALLINT:
                case INT:
                    resType = DataType.INT;

                    break;

                case BIGINT:
                    resType = DataType.BIGINT;

                    break;

                case DECIMAL:
                    resType = DataType.DECIMAL;

                    break;

                case REAL:
                case DOUBLE:
                    resType = DataType.DOUBLE;

                    break;

                default:
                    throw new HazelcastSqlException(-1, "Unsupported type of the first operand: " + operand1.getType());
            }

            valType = type;
        }

        return val;
    }

    /**
     * Get length (second operand).
     *
     * @param ctx Context.
     * @param row Row.
     * @return Length.
     */
    private int getLength(QueryContext ctx, Row row) {
        Object len = operand2 != null ? operand2.eval(ctx, row) : null;

        if (len == null) {
            return 0;
        }

        if (lenType == null) {
            DataType type = operand2.getType();

            switch (type.getType()) {
                case BIT:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    lenType = type;

                    break;

                default:
                    throw new HazelcastSqlException(-1, "Unsupported type of the second operand: "
                        + operand2.getType());
            }
        }

        return lenType.getConverter().asInt(len);
    }

    /**
     * Perform actual round/truncate.
     *
     * @param val Value.
     * @param len Length.
     * @return Rounded value.
     */
    private Object doRoundTruncate(Object val, int len) {
        BigDecimal res = valType.getConverter().asDecimal(val);

        RoundingMode roundingMode = truncate ? RoundingMode.DOWN : RoundingMode.HALF_UP;

        if (len == 0) {
            res = res.setScale(0, roundingMode);
        } else {
            res = res.movePointRight(len).setScale(0, roundingMode).movePointLeft(len);
        }

        try {
            switch (resType.getType()) {
                case INT:
                    return res.intValueExact();

                case BIGINT:
                    return res.longValueExact();

                case DECIMAL:
                    return res;

                case DOUBLE:
                    return res.doubleValue();

                default:
                    throw new HazelcastSqlException(-1, "Unexpected result type: " + resType);
            }
        } catch (ArithmeticException e) {
            throw new HazelcastSqlException(-1, "Data overflow.");
        }
    }

    @Override
    public int operator() {
        return truncate ? CallOperator.TRUNCATE : CallOperator.ROUND;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(truncate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        truncate = in.readBoolean();
    }
}
