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

package com.hazelcast.sql.impl.expression.util;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Provides a set of utilities for evaluating expressions with subsequent
 * conversion of the evaluation result to one type or another.
 */
public final class Eval {
    private Eval() {
        // No-op.
    }

    /**
     * Evaluates the given expression on the given row and then converts the
     * evaluation result to BOOLEAN type.
     *
     * @param expression the expression to evaluate.
     * @param row        the row to evaluate the expression on.
     * @return the converted evaluation result.
     */
    @SuppressFBWarnings(value = "NP_BOOLEAN_RETURN_NULL", justification = "SQL ternary logic allows NULL")
    public static Boolean asBoolean(Expression<?> expression, Row row, ExpressionEvalContext context) {
        Object res = expression.eval(row, context);

        if (res == null) {
            return null;
        }

        return expression.getType().getConverter().asBoolean(res);
    }
}
