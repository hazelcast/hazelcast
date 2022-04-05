/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.TriExpression;
import com.hazelcast.sql.impl.expression.math.MathFunctionUtils;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

public class SubstringFunction extends TriExpression<String> implements IdentifiedDataSerializable {
    public SubstringFunction() {
        // No-op
    }

    private SubstringFunction(Expression<?> input, Expression<?> start, Expression<?> length) {
        super(input, start, length);
    }

    public static SubstringFunction create(Expression<?> input, Expression<?> start, Expression<?> length) {
        return new SubstringFunction(input, start, length);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    @Override
    public String eval(Row row, ExpressionEvalContext context) {
        // Get input
        String input;

        try {
            input = StringFunctionUtils.asVarchar(operand1, row, context);
        } catch (Exception e) {
            // Conversion to String failed. E.g. NPE on UserClass.toString()
            throw QueryException.dataException(
                "Failed to get value of input operand of SUBSTRING function: " + e.getMessage(), e
            );
        }

        if (input == null) {
            // NULL always yields NULL
            return null;
        }

        Integer start = MathFunctionUtils.asInt(operand2, row, context);

        if (start == null) {
            return null;
        }

        if (start < 1) {
            // Different databases provide different semantics on negative values. Oracle start counting
            // from the end, SQL Server starts from the beginning, and uses the value to calculate the
            // final length, etc.
            // These semantics are pretty complicated, so wi disallow it completely.
            throw QueryException.dataException("SUBSTRING \"start\" operand must be positive");
        }

        // In SQL start position is 1-based. Convert it to 0-based for Java.
        int adjustedStart = start - 1;

        if (adjustedStart >= input.length()) {
            // Start position is beyond the string length, e.g. SUBSTRING("abc", 4)
            return "";
        }

        Integer length;

        if (operand3 != null) {
            length = MathFunctionUtils.asInt(operand3, row, context);

            if (length == null) {
                return null;
            }
        } else {
            length = null;
        }

        if (length == null) {
            // Length is not specified, just cut from the start
            return adjustedStart > 0 ? input.substring(adjustedStart) : input;
        } else if (length < 0) {
            // Negative lengths are not allowed
            throw QueryException.dataException("SUBSTRING \"length\" operand cannot be negative");
        } else if (length == 0) {
            // Zero length always yields empty string
            return "";
        } else if (adjustedStart + length > input.length()) {
            // Length is outside of input limits, ignore
            return adjustedStart > 0 ? input.substring(adjustedStart) : input;
        } else {
            // Length is within input limits, use it
            return input.substring(adjustedStart, adjustedStart + length);
        }
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_SUBSTRING;
    }
}
