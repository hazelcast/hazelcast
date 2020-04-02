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

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("rawtypes")
public class SortPlanNode extends UniInputPlanNode {
    /** Expressions. */
    private List<Expression> expressions;

    /** Sort orders. */
    private List<Boolean> ascs;

    /** Limit. */
    private Expression fetch;

    /** Offset. */
    private Expression offset;

    public SortPlanNode() {
        // No-op.
    }

    public SortPlanNode(
        int id,
        PlanNode upstream,
        List<Expression> expressions,
        List<Boolean> ascs,
        Expression fetch,
        Expression offset
    ) {
        super(id, upstream);

        this.expressions = expressions;
        this.ascs = ascs;
        this.fetch = fetch;
        this.offset = offset;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    public List<Boolean> getAscs() {
        return ascs;
    }

    public Expression getFetch() {
        return fetch;
    }

    public Expression getOffset() {
        return offset;
    }

    @Override
    public void visit0(PlanNodeVisitor visitor) {
        visitor.onSortNode(this);
    }

    @Override
    public void writeData1(ObjectDataOutput out) throws IOException {
        out.writeObject(expressions);
        out.writeObject(ascs);
        out.writeObject(fetch);
        out.writeObject(offset);
    }

    @Override
    public void readData1(ObjectDataInput in) throws IOException {
        expressions = in.readObject();
        ascs = in.readObject();
        fetch = in.readObject();
        offset = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, expressions, ascs, fetch, offset, upstream);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SortPlanNode that = (SortPlanNode) o;

        return id == that.id && expressions.equals(that.expressions) && ascs.equals(that.ascs)
            && Objects.equals(fetch, that.fetch) && Objects.equals(offset, that.offset) && upstream.equals(that.upstream);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", expressions=" + expressions + ", ascs=" + ascs
            + ", fetch=" + fetch + ", offset=" + offset + ", upstream=" + upstream + '}';
    }
}
