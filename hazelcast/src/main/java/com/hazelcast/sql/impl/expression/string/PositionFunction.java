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

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.TriCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * POSITION(seek IN source FROM position)}.
 */
public class PositionFunction extends TriCallExpression<Integer> {
    public PositionFunction() {
        // No-op.
    }

    private PositionFunction(Expression<?> seek, Expression<?> source, Expression<?> position) {
        super(seek, source, position);
    }

    public static PositionFunction create(Expression<?> seek, Expression<?> source, Expression<?> position) {
        seek.ensureCanConvertToVarchar();
        source.ensureCanConvertToVarchar();

        if (position != null) {
            position.ensureCanConvertToInt();
        }

        return new PositionFunction(seek, source, position);
    }

    @Override
    public Integer eval(Row row) {
        String seek = operand1.evalAsVarchar(row);
        String source = operand2.evalAsVarchar(row);
        Integer position = operand3 != null ? operand3.evalAsInt(row) : null;

        return StringExpressionUtils.position(seek, source, position != null ? position : 0);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.INT;
    }
}
