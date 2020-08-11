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
 * Base class for expressions acting on a single operand and having a variable
 * result type.
 */
public abstract class UniExpressionWithType<T> extends UniExpression<T> {

    protected QueryDataType resultType;

    protected UniExpressionWithType() {
        // No-op.
    }

    protected UniExpressionWithType(Expression<?> operand, QueryDataType resultType) {
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

        UniExpressionWithType<?> that = (UniExpressionWithType<?>) o;

        return resultType.equals(that.resultType);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{operand=" + operand + ", resultType=" + resultType + '}';
    }

}
