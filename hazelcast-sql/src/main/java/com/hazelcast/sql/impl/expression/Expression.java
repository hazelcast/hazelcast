/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.expression;

import com.hazelcast.jet.impl.execution.CooperativeThread;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Serializable;

/**
 * Defines expression contract for SQL.
 * <p>
 * Java serialization is needed for Jet.
 *
 * @param <T> the return type of this expression.
 */
@ThreadSafe
public interface Expression<T> extends IdentifiedDataSerializable, Serializable {
    /**
     * Evaluates this expression, guaranteeing that this is a top-level call.
     *
     * @param row the row to evaluate this expression on
     * @param context the expression evaluation context
     * @return the result produced by the evaluation
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
     * @param row     the row to evaluate this expression on
     * @param context the expression evaluation context
     * @return the result produced by the evaluation
     */
    T eval(Row row, ExpressionEvalContext context);

    /**
     * Evaluates this expression. By default, this method ignores useLazyDeserialization parameter,
     * i.e, expression that uses the default implementation won't use lazy deserialization.
     *
     * @param row                    the row to evaluate this expression on
     * @param context                the expression evaluation context
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

    /**
     * Returns a boolean flag whether this expression is allowed to be evaluated
     * in cooperative processor.
     * <p>
     * Expressions returning false directly (i.e. not because their operands
     * return false) should call {@link
     * CooperativeThread#checkNonCooperative()}.
     */
    boolean isCooperative();

    @Override
    default int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }
}
