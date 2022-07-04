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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;

/**
 * Implements ternary boolean logic according to SQL standard.
 * <p>
 * SQL BOOLEAN type is represented by Java {@link Boolean} type, SQL BOOLEAN NULL,
 * also known as UNKNOWN, is represented as Java {@code null}.
 */
public final class TernaryLogic {

    private TernaryLogic() {
        // No-op.
    }

    /**
     * Performs AND for the given operands acting on the given row in the given
     * context.
     * <p>
     * The method exhibits a short-circuiting behaviour: there is no guarantee
     * all of the passed operands would be evaluated. Operand evaluation order
     * is unspecified.
     *
     * @param row      the row to evaluate the operands on.
     * @param context  the context to evaluate the operands in.
     * @param operands the boolean operands to evaluate.
     * @return {@code true} if all of the operands were evaluated to {@code true},
     * {@code false} if at least one of the operands was evaluated to {@code false},
     * {@code null} otherwise.
     */
    public static Boolean and(Row row, ExpressionEvalContext context, Expression<?>... operands) {
        boolean seenUnknown = false;

        for (Expression<?> operand : operands) {
            Boolean result = (Boolean) operand.eval(row, context);

            if (isFalse(result)) {
                return Boolean.FALSE;
            }

            if (isNull(result)) {
                seenUnknown = true;
            }
        }

        return seenUnknown ? null : Boolean.TRUE;
    }

    /**
     * Performs OR for the given operands acting on the given row in the given
     * context.
     * <p>
     * The method exhibits a short-circuiting behaviour: there is no guarantee
     * all of the passed operands would be evaluated. Operand evaluation order
     * is unspecified.
     *
     * @param row      the row to evaluate the operands on.
     * @param context  the context to evaluate the operands in.
     * @param operands the boolean operands to evaluate.
     * @return {@code false} if all of the operands were evaluated to {@code false},
     * {@code true} if at least one of the operands was evaluated to {@code true},
     * {@code null} otherwise.
     */
    public static Boolean or(Row row, ExpressionEvalContext context, Expression<?>... operands) {
        boolean seenUnknown = false;

        for (Expression<?> operand : operands) {
            Boolean result = (Boolean) operand.eval(row, context);

            if (isTrue(result)) {
                return Boolean.TRUE;
            }

            if (isNull(result)) {
                seenUnknown = true;
            }
        }

        return seenUnknown ? null : Boolean.FALSE;
    }

    /**
     * Negates the given boolean value.
     *
     * @param value the value to negate.
     * @return {@code true} if the passed value was {@code false}, {@code false}
     * if it was {@code true}, {@code null} if it was {@code null}.
     */
    public static Boolean not(Boolean value) {
        return value == null ? null : !value;
    }

    /**
     * Checks whether the given value is {@code null}.
     *
     * @param value the value to check.
     * @return {@code true} if the passed value is {@code null}, {@code false}
     * otherwise.
     */
    public static boolean isNull(Object value) {
        return value == null;
    }

    /**
     * Checks whether the given value is not {@code null}.
     *
     * @param value the value to check.
     * @return {@code false} if the passed value is {@code null}, {@code true}
     * otherwise.
     */
    public static boolean isNotNull(Object value) {
        return value != null;
    }

    /**
     * Checks whether the given value is {@code true}.
     *
     * @param value the value to check.
     * @return {@code true} if the passed value is {@code true}; {@code false}
     * otherwise, i.e. the passed value is either {@code false} or {@code null}.
     */
    public static boolean isTrue(Boolean value) {
        return value != null && value;
    }

    /**
     * Checks whether the given value is not {@code true}.
     *
     * @param value the value to check.
     * @return {@code true} if the passed value is either {@code false} or
     * {@code null}; {@code false} otherwise, i.e. the passed value is {@code
     * true}.
     */
    public static boolean isNotTrue(Boolean value) {
        return value == null || !value;
    }

    /**
     * Checks whether the given value is {@code false}.
     *
     * @param value the value to check.
     * @return {@code true} if the passed value is {@code false}; {@code false}
     * otherwise, i.e. the passed value is either {@code true} or {@code null}.
     */
    public static boolean isFalse(Boolean value) {
        return value != null && !value;
    }

    /**
     * Checks whether the given value is not {@code false}.
     *
     * @param value the value to check.
     * @return {@code true} if the passed value is either {@code true} or
     * {@code null}; {@code false} otherwise, i.e. the passed value is {@code
     * false}.
     */
    public static boolean isNotFalse(Boolean value) {
        return value == null || value;
    }

}
