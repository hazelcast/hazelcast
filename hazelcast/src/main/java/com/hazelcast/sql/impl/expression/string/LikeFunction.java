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
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.util.EnsureConvertible;
import com.hazelcast.sql.impl.expression.util.Eval;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.TriExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

/**
 * LIKE string function.
 */
public class LikeFunction extends TriExpression<Boolean> {

    private static final long serialVersionUID = -3795317922395825892L;

    /** Executor. */
    private transient LikeFunctionExecutor like = new LikeFunctionExecutor();

    public LikeFunction() {
        // No-op.
    }

    private LikeFunction(Expression<?> source, Expression<?> pattern, Expression<?> escape) {
        super(source, pattern, escape);
    }

    public static LikeFunction create(Expression<?> source, Expression<?> pattern, Expression<?> escape) {
        EnsureConvertible.toVarchar(source);
        EnsureConvertible.toVarchar(pattern);

        if (escape != null) {
            EnsureConvertible.toVarchar(escape);
        }

        return new LikeFunction(source, pattern, escape);
    }

    @Override
    public Boolean eval(Row row, ExpressionEvalContext context) {
        String source = Eval.asVarchar(operand1, row, context);
        String pattern = Eval.asVarchar(operand2, row, context);
        String escape = operand3 != null ? Eval.asVarchar(operand3, row, context) : null;

        return like.like(source, pattern, escape);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.BOOLEAN;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        like = new LikeFunctionExecutor();
    }
}
