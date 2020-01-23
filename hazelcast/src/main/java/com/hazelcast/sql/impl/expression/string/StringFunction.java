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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Family of string functions.
 */
public class StringFunction<T> extends UniCallExpression<T> {
    /** Operator. */
    private int operator;

    /** Operand type. */
    private transient DataType operandType;

    public StringFunction() {
        // No-op.
    }

    public StringFunction(Expression operand, int operator) {
        super(operand);

        this.operator = operator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row) {
        Object operandValue = operand.eval(row);

        if (operandValue == null) {
            return null;
        }

        if (operandType == null) {
            operandType = operand.getType();
        }

        String operandValueString = operandType.getConverter().asVarchar(operandValue);

        return (T) eval0(operandValueString);
    }

    /**
     * Evaluate the operand and get back the result.
     *
     * @param operand Operand.
     * @return Result.
     */
    private Object eval0(String operand) {
        switch (operator) {
            case CallOperator.CHAR_LENGTH:
                return operand.length();

            case CallOperator.ASCII:
                return operand.isEmpty() ? 0 : operand.codePointAt(0);

            case CallOperator.UPPER:
                return operand.toUpperCase(Locale.ROOT);

            case CallOperator.LOWER:
                return operand.toLowerCase(Locale.ROOT);

            case CallOperator.INITCAP:
                return doCapitalize(operand);

            default:
                throw new HazelcastSqlException(-1, "Unsupported operator: " + operator);
        }
    }

    /**
     * Do capitalization.
     *
     * @param operandValue Input string.
     * @return Result.
     */
    private static String doCapitalize(String operandValue) {
        if (operandValue.length() == 0) {
            return operandValue;
        }

        int strLen = operandValue.length();

        StringBuilder res = new StringBuilder(strLen);

        boolean capitalizeNext = true;

        for (int i = 0; i < strLen; i++) {
            char c = operandValue.charAt(i);

            if (Character.isWhitespace(c)) {
                res.append(c);

                capitalizeNext = true;
            } else if (capitalizeNext) {
                res.append(Character.toTitleCase(c));

                capitalizeNext = false;
            } else {
                res.append(c);
            }
        }

        return res.toString();
    }

    @Override
    public DataType getType() {
        switch (operator) {
            case CallOperator.CHAR_LENGTH:
            case CallOperator.ASCII:
                return DataType.INT;

            case CallOperator.UPPER:
            case CallOperator.LOWER:
            case CallOperator.INITCAP:
                return DataType.VARCHAR;

            default:
                throw new HazelcastSqlException(-1, "Unsupported operator: " + operator);
        }
    }

    @Override
    public int operator() {
        return operator;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeInt(operator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        operator = in.readInt();
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, operand);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StringFunction<?> that = (StringFunction<?>) o;

        return operator == that.operator && operand.equals(that.operand);
    }

    @Override
    public String toString() {
        return "StringFunction{operator=" + operator + ", operand=" + operand + '}';
    }
}
