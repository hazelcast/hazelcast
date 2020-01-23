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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;

/**
 * Expression which returns an argument by index.
 *
 * @param <T> Return type.
 */
public class ArgumentExpression<T> implements Expression<T> {
    /** Index. */
    private int idx;

    /** Return type. */
    private transient DataType type;

    public ArgumentExpression() {
        // No-op.
    }

    public ArgumentExpression(int idx) {
        this.idx = idx;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        Object res = ctx.getArgument(idx);

        if (res != null && type == null) {
            type = DataType.resolveType(res);
        }

        return (T) res;
    }

    @Override
    public DataType getType() {
        return DataType.notNullOrLate(type);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(idx);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        idx = in.readInt();
    }
}
