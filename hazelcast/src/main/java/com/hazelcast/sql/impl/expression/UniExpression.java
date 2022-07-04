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

/**
 * Expression with one operand.
 */
public abstract class UniExpression<T> implements Expression<T> {

    protected Expression<?> operand;

    protected UniExpression() {
        // No-op.
    }

    protected UniExpression(Expression<?> operand) {
        this.operand = operand;
    }

    public Expression<?> getOperand() {
        return operand;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(operand);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        operand = in.readObject();
    }

    @Override
    public int hashCode() {
        return operand.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UniExpression<?> that = (UniExpression<?>) o;

        return operand.equals(that.operand);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{operand=" + operand + '}';
    }
}
