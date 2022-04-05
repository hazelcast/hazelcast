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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

public final class MathFunctionUtils {
    private MathFunctionUtils() {
        // No-op.
    }

    public static boolean notFractional(QueryDataType type) {
        switch (type.getTypeFamily()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return true;

            default:
                return false;
        }
    }

    public static Integer asInt(Expression<?> expression, Row row, ExpressionEvalContext context) {
        Object res = expression.eval(row, context);

        if (res == null) {
            return null;
        }

        return expression.getType().getConverter().asInt(res);
    }

    public static Long asBigint(Expression<?> expression, Row row, ExpressionEvalContext context) {
        Object res = expression.eval(row, context);

        if (res == null) {
            return null;
        }

        return expression.getType().getConverter().asBigint(res);
    }
}
