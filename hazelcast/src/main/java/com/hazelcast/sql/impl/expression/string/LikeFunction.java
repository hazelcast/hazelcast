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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.TriCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

/**
 * LIKE string function.
 */
public class LikeFunction extends TriCallExpression<Boolean> {
    /** Executor. */
    private transient LikeFunctionExecutor like = new LikeFunctionExecutor();

    public LikeFunction() {
        // No-op.
    }

    private LikeFunction(Expression<?> source, Expression<?> pattern, Expression<?> escape) {
        super(source, pattern, escape);
    }

    public static LikeFunction create(Expression<?> source, Expression<?> pattern, Expression<?> escape) {
        source.ensureCanConvertToVarchar();
        pattern.ensureCanConvertToVarchar();

        if (escape != null) {
            escape.ensureCanConvertToVarchar();
        }

        return new LikeFunction(source, pattern, escape);
    }

    @Override
    public Boolean eval(Row row) {
        String source = operand1.evalAsVarchar(row);
        String pattern = operand2.evalAsVarchar(row);
        String escape = operand3 != null ? operand3.evalAsVarchar(row) : null;

        return like.like(source, pattern, escape);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.BIT;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        like = new LikeFunctionExecutor();
    }
}
