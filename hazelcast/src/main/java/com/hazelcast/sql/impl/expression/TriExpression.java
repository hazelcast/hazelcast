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
import java.util.Objects;

/**
 * Expression with three operands.
 */
public abstract class TriExpression<T> implements Expression<T> {

    protected Expression<?> operand1;
    protected Expression<?> operand2;
    protected Expression<?> operand3;

    protected TriExpression() {
        // No-op.
    }

    protected TriExpression(Expression<?> operand1, Expression<?> operand2, Expression<?> operand3) {
        this.operand1 = operand1;
        this.operand2 = operand2;
        this.operand3 = operand3;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(operand1);
        out.writeObject(operand2);
        out.writeObject(operand3);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        operand1 = in.readObject();
        operand2 = in.readObject();
        operand3 = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TriExpression<?> that = (TriExpression<?>) o;

        return Objects.equals(operand1, that.operand1)
            && Objects.equals(operand2, that.operand2)
            && Objects.equals(operand3, that.operand3);
    }

    @Override
    public int hashCode() {
        int result = operand1 != null ? operand1.hashCode() : 0;
        result = 31 * result + (operand2 != null ? operand2.hashCode() : 0);
        result = 31 * result + (operand3 != null ? operand3.hashCode() : 0);
        return result;
    }
}
