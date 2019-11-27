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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.BiCallExpression;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;
import java.util.Objects;

/**
 * Predicates: IS NULL / IS NOT NULL / IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE
 */
public class AndOrPredicate extends BiCallExpression<Boolean> {
    /** Operator. */
    private boolean or;

    /** Whether the first operand is checked. */
    private transient boolean operand1Checked;

    /** Whether the second operand is checked. */
    private transient boolean operand2Checked;

    public AndOrPredicate() {
        // No-op.
    }

    public AndOrPredicate(Expression operand1, Expression operand2, boolean or) {
        super(operand1, operand2);

        this.or = or;
    }

    @Override
    public Boolean eval(QueryContext ctx, Row row) {
        Object operand1Value = operand1.eval(ctx, row);
        Object operand2Value = operand2.eval(ctx, row);

        if (operand1Value != null && !operand1Checked) {
            if (operand1.getType() != DataType.BIT) {
                throw new HazelcastSqlException(-1, "Operand 1 is not BIT.");
            }

            operand1Checked = true;
        }

        if (operand2Value != null && !operand2Checked) {
            if (operand2.getType() != DataType.BIT) {
                throw new HazelcastSqlException(-1, "Operand 2 is not BIT.");
            }

            operand2Checked = true;
        }

        return eval0((Boolean) operand1Value, (Boolean) operand2Value);
    }

    private Boolean eval0(Boolean first, Boolean second) {
        if (or) {
            if (first == null) {
                return second;
            } else if (second == null) {
                return null;
            } else {
                return first || second;
            }
        } else {
            if (first == null || second == null) {
                return null;
            } else {
                return first && second;
            }
        }
    }

    @Override
    public DataType getType() {
        return DataType.BIT;
    }

    @Override
    public int operator() {
        return or ? CallOperator.OR : CallOperator.AND;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(or);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        or = in.readBoolean();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AndOrPredicate that = (AndOrPredicate) o;

        return or == that.or && Objects.equals(operand1, that.operand1) && Objects.equals(operand2, that.operand2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(or, operand1, operand2);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{or=" + or + ", operand1=" + operand1 + ", operand2=" + operand2 + '}';
    }
}
