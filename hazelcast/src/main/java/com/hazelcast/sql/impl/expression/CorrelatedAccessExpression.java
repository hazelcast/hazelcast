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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

/**
 * An expression backing the DOT operator for extracting correlated expression value.
 * <p>
 * {@code $cor0.field} - extracts `field` from correlation expression 0.
 */
public class CorrelatedAccessExpression<T> implements Expression<T>, IdentifiedDataSerializable {
    private QueryDataType type;
    private Integer id;
    private Expression<?> ref;

    public CorrelatedAccessExpression() { }

    private CorrelatedAccessExpression(
            final QueryDataType type,
            final Integer id,
            final Expression<?> ref
    ) {
        this.type = type;
        this.id = id;
        this.ref = ref;
    }

    public static CorrelatedAccessExpression<?> create(
            final QueryDataType type,
            final Integer id,
            final Expression<?> ref
    ) {
        return new CorrelatedAccessExpression<>(type, id, ref);
    }


    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        return eval(row, context, false);
    }

    @Override
    public T eval(final Row row, final ExpressionEvalContext context, boolean useLazyDeserialization) {
        CorrelatedExpressionEvalContext correlatedContext = (CorrelatedExpressionEvalContext) context;
        // Use lazy deserialization for nested queries. Only the last access should be eager.
        return (T) ref.eval(correlatedContext.getCorrelationVariable(id).getRow(), context, true);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.CORRELATED_FIELD_ACCESS;
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeObject(type);
        out.writeInt(id);
        out.writeObject(ref);
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        type = in.readObject();
        id = in.readInt();
        ref = in.readObject();
    }

    @Override
    public QueryDataType getType() {
        return type;
    }
}
