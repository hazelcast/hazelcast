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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.util.Eval;
import com.hazelcast.sql.impl.row.Row;

public final class TernaryLogic {

    private TernaryLogic() {
        // No-op.
    }

    public static Boolean and(Row row, ExpressionEvalContext context, Expression<?>... operands) {
        boolean seenUnknown = false;

        for (Expression<?> operand : operands) {
            Boolean result = Eval.asBoolean(operand, row, context);

            if (result == Boolean.FALSE) {
                return Boolean.FALSE;
            }

            if (result == null) {
                seenUnknown = true;
            }
        }

        return seenUnknown ? null : Boolean.TRUE;
    }

    public static Boolean or(Row row, ExpressionEvalContext context, Expression<?>... operands) {
        boolean seenUnknown = false;

        for (Expression<?> operand : operands) {
            Boolean result = Eval.asBoolean(operand, row, context);

            if (result == Boolean.TRUE) {
                return Boolean.TRUE;
            }

            if (result == null) {
                seenUnknown = true;
            }
        }

        return seenUnknown ? null : Boolean.FALSE;
    }

    public static Boolean not(Boolean value) {
        return value == null ? null : !value;
    }

    public static boolean isNull(Object value) {
        return value == null;
    }

    public static boolean isNotNull(Object value) {
        return value != null;
    }

    public static boolean isTrue(Boolean value) {
        return value == Boolean.TRUE;
    }

    public static boolean isNotTrue(Boolean value) {
        return value != Boolean.TRUE;
    }

    public static boolean isFalse(Boolean value) {
        return value == Boolean.FALSE;
    }

    public static boolean isNotFalse(Boolean value) {
        return value != Boolean.FALSE;
    }

}
