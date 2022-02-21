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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * Base class for expressions acting on variable number of operands.
 */
public abstract class VariExpression<T> implements Expression<T> {

    protected Expression<?>[] operands;

    protected VariExpression() {
        // do nothing
    }

    protected VariExpression(Expression<?>... operands) {
        this.operands = operands;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(operands.length);
        for (Expression<?> operand : operands) {
            out.writeObject(operand);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int length = in.readInt();
        operands = new Expression<?>[length];
        for (int i = 0; i < length; ++i) {
            operands[i] = in.readObject();
        }
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(operands);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VariExpression<?> that = (VariExpression<?>) o;

        return Arrays.equals(this.operands, that.operands);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{operands=" + Arrays.toString(operands) + '}';
    }

}
