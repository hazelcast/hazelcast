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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.TriCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

/**
 * SUBSTRING string function.
 */
public class SubstringFunction extends TriCallExpression<String> {
    /** Source type. */
    private transient DataType sourceType;

    /** Start type. */
    private transient DataType startType;

    /** Length type. */
    private transient DataType lengthType;

    public SubstringFunction() {
        // No-op.
    }

    public SubstringFunction(Expression operand1, Expression operand2, Expression operand3) {
        super(operand1, operand2, operand3);
    }

    @Override
    public String eval(QueryContext ctx, Row row) {
        String source;
        int start;
        int length;

        // Get source operand.
        Object sourceValue = operand1.eval(ctx, row);

        if (sourceValue == null) {
            return null;
        }

        if (sourceType == null) {
            sourceType = operand1.getType();
        }

        source = sourceType.getConverter().asVarchar(sourceValue);

        // Get search operand.
        Object startValue = operand2.eval(ctx, row);

        if (startValue == null) {
            return null;
        }

        if (startType == null) {
            startType = operand2.getType();
        }

        start = startType.getConverter().asInt(startValue);

        // Get replacement operand.
        Object lengthValue = operand3.eval(ctx, row);

        if (lengthValue == null) {
            return null;
        }

        if (lengthType == null) {
            lengthType = operand3.getType();
        }

        length = lengthType.getConverter().asInt(lengthValue);

        // Process.
        return substring(source, start, length);
    }

    // TODO: Validate the implementation against ANSI.
    private static String substring(String source, int startPos, int length) {
        int sourceLength = source.length();

        if (startPos < 0) {
            startPos += sourceLength + 1;
        }

        int endPos = startPos + length;

        if (endPos < startPos) {
            throw new HazelcastSqlException(-1, "End position is less than start position.");
        }

        if (startPos > sourceLength || endPos < 1) {
            return "";
        }

        int startPos0 = Math.max(startPos, 1);
        int endPos0 = Math.min(endPos, sourceLength + 1);

        return source.substring(startPos0 - 1, endPos0 - 1);
    }

    @Override
    public int operator() {
        return CallOperator.SUBSTRING;
    }

    @Override
    public DataType getType() {
        return DataType.VARCHAR;
    }
}
