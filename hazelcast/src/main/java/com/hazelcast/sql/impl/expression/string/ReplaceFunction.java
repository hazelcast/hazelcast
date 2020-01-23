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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.TriCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

/**
 * REPLACE string function.
 */
public class ReplaceFunction extends TriCallExpression<String> {
    /** Source type. */
    private transient DataType sourceType;

    /** Search type. */
    private transient DataType searchType;

    /** Replacement type. */
    private transient DataType replacementType;

    public ReplaceFunction() {
        // No-op.
    }

    public ReplaceFunction(Expression operand1, Expression operand2, Expression operand3) {
        super(operand1, operand2, operand3);
    }

    @Override
    public String eval(QueryContext ctx, Row row) {
        String source;
        String search;
        String replacement;

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
        Object searchValue = operand2.eval(ctx, row);

        if (searchValue == null) {
            return null;
        }

        if (searchType == null) {
            searchType = operand2.getType();
        }

        search = searchType.getConverter().asVarchar(searchValue);

        if (search.isEmpty()) {
            throw new HazelcastSqlException(-1, "Invalid operand: search cannot be empty.");
        }

        // Get replacement operand.
        Object replacementValue = operand3.eval(ctx, row);

        if (replacementValue == null) {
            return null;
        }

        if (replacementType == null) {
            replacementType = operand3.getType();
        }

        replacement = replacementType.getConverter().asVarchar(replacementValue);

        // Process.
        return source.replace(search, replacement);
    }

    @Override
    public int operator() {
        return CallOperator.REPLACE;
    }

    @Override
    public DataType getType() {
        return DataType.VARCHAR;
    }
}
