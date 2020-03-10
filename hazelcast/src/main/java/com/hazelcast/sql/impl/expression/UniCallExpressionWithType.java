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
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Objects;

/**
 * Expression with result type field.
 */
public abstract class UniCallExpressionWithType<T> extends UniCallExpression<T> {
    /** Result type. */
    protected QueryDataType resultType;

    protected UniCallExpressionWithType() {
        // No-op.
    }

    protected UniCallExpressionWithType(Expression<?> operand, QueryDataType resultType) {
        this.operand = operand;
        this.resultType = resultType;
    }

    @Override
    public QueryDataType getType() {
        return resultType;
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeObject(resultType);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        resultType = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        UniCallExpressionWithType<?> that = (UniCallExpressionWithType<?>) o;

        return resultType.equals(that.resultType);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{operand=" + operand + ", resultType=" + resultType + '}';
    }
}
