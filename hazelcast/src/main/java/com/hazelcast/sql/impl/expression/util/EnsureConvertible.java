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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;

/**
 * Provides a set of utilities for ensuring expressions are convertible to one
 * type or another.
 */
public final class EnsureConvertible {

    private EnsureConvertible() {
        // do nothing
    }

    /**
     * Ensures that the given expression is convertible to BOOLEAN type.
     *
     * @param expression the expression to ensure the conversion of.
     * @throws QueryException if the expression is not convertible.
     */
    public static void toBoolean(Expression<?> expression) {
        if (!expression.getType().getConverter().canConvertToBoolean()) {
            throw QueryException.error("Expression cannot be converted to BOOLEAN: " + expression);
        }
    }

    /**
     * Ensures that the given expression is convertible to INT type.
     *
     * @param expression the expression to ensure the conversion of.
     * @throws QueryException if the expression is not convertible.
     */
    public static void toInt(Expression<?> expression) {
        if (!expression.getType().getConverter().canConvertToInt()) {
            throw QueryException.error("Expression cannot be converted to INT: " + expression);
        }
    }

    /**
     * Ensures that the given expression is convertible to VARCHAR type.
     *
     * @param expression the expression to ensure the conversion of.
     * @throws QueryException if the expression is not convertible.
     */
    public static void toVarchar(Expression<?> expression) {
        if (!expression.getType().getConverter().canConvertToVarchar()) {
            throw QueryException.error("Expression cannot be converted to VARCHAR: " + expression);
        }
    }

    /**
     * Ensures that the given expression is convertible to TIMESTAMP WITH TIME
     * ZONE type.
     *
     * @param expression the expression to ensure the conversion of.
     * @throws QueryException if the expression is not convertible.
     */
    public static void toTimestampWithTimezone(Expression<?> expression) {
        if (!expression.getType().getConverter().canConvertToVarchar()) {
            throw QueryException.error("Expression cannot be converted to TIMESTAMP WITH TIME ZONE: " + expression);
        }
    }

}
