/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.Serializable;

/**
 * Defines expression contract for SQL.
 * <p>
 * Java serialization is needed for Jet.
 *
 * @param <T> the return type of this expression.
 */
public interface Expression<T> extends DataSerializable, Serializable {
    /**
     * Evaluates this expression, guaranteeing that this is a top-level call.
     *
     * @param row the row to evaluate this expression on
     *      * @param context the expression evaluation context
     *      * @return the result produced by the evaluation
     */
    default Object evalTop(Row row, ExpressionEvalContext context) {
        // If we are evaluating the expression as top, don't use lazy deserialization because
        // compact and portable generic records need to be in deserialized form when returned to
        // the user.
        return eval(row, context, false);
    }

    /**
     * Evaluates this expression.
     *
     * @param row the row to evaluate this expression on
     * @param context the expression evaluation context
     * @return the result produced by the evaluation
     */
    T eval(Row row, ExpressionEvalContext context);

    /**
     * Evaluates this expression. By default, this method ignores useLazyDeserialization parameter,
     * i.e, expression that uses the default implementation won't use lazy deserialization.
     *
     * @param row the row to evaluate this expression on
     * @param context the expression evaluation context
     * @param useLazyDeserialization whether to use lazy deserialization
     * @return the result produced by the evaluation
     */
    default T eval(Row row, ExpressionEvalContext context, boolean useLazyDeserialization) {
        return eval(row, context);
    }

    /**
     * @return the return query data type of this expression.
     */
    QueryDataType getType();
}
