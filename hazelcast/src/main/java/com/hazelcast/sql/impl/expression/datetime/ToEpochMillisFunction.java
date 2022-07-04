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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.time.OffsetDateTime;

public final class ToEpochMillisFunction extends UniExpression<Long> implements IdentifiedDataSerializable {
    public ToEpochMillisFunction() { }

    private ToEpochMillisFunction(Expression<?> operand) {
        super(operand);
    }

    public static ToEpochMillisFunction create(Expression<?> operand) {
        return new ToEpochMillisFunction(operand);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_TO_EPOCH_MILLIS;
    }

    @Override
    public Long eval(final Row row, final ExpressionEvalContext context) {
        final OffsetDateTime dateTime = (OffsetDateTime) this.operand.eval(row, context);
        if (dateTime == null) {
            return null;
        }

        return dateTime.toInstant().toEpochMilli();
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.BIGINT;
    }
}
