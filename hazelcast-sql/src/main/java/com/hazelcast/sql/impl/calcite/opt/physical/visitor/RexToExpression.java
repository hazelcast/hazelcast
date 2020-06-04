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

package com.hazelcast.sql.impl.calcite.opt.physical.visitor;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.predicate.IsNullPredicate;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;

/**
 * Utility methods for REX to Hazelcast expression conversion.
 */
public final class RexToExpression {
    private RexToExpression() {
        // No-op.
    }

    /**
     * Converts a {@link RexCall} to {@link Expression}.
     *
     * @param call the call to convert.
     * @return the resulting expression.
     * @throws QueryException if the given {@link RexCall} can't be
     *                               converted.
     */
    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    public static Expression<?> convertCall(RexCall call, Expression<?>[] operands) {
        SqlOperator operator = call.getOperator();

        switch (operator.getKind()) {
            case IS_NULL:
                return IsNullPredicate.create(operands[0]);

            default:
                break;
        }

        throw QueryException.error("Unsupported operator: " + operator);
    }
}
