/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
 * Constant expression.
 *
 * @param <T> Return type.
 */
public class ConstantExpression<T> implements Expression<T> {
    /** Value. */
    private T val;

    /** Return type. */
    private transient DataType type;

    public ConstantExpression() {
        // No-op.
    }

    public ConstantExpression(T val) {
        this.val = val;
    }

    @Override
    public T eval(QueryContext ctx, Row row) {
        if (val != null && type == null) {
            type = DataType.resolveType(val);
        }

        return val;
    }

    @Override
    public DataType getType() {
        return DataType.notNullOrLate(type);
    }

    public T getValue() {
        return val;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(val);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        val = in.readObject();
    }
}
