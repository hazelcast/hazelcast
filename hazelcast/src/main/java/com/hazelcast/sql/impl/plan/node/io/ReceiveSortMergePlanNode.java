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

package com.hazelcast.sql.impl.plan.node.io;

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.plan.node.ZeroInputPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical node which receives from remote stripes and performs sort-merge.
 */
@SuppressWarnings("rawtypes")
public class ReceiveSortMergePlanNode extends ZeroInputPlanNode implements EdgeAwarePlanNode {
    /** Edge iD. */
    private int edgeId;

    /** Field types. */
    private List<QueryDataType> fieldTypes;

    /** Expressions to be used for sorting. */
    private List<Expression> expressions;

    /** Sort directions. */
    private List<Boolean> ascs;

    /** Limit. */
    private Expression fetch;

    /** Offset. */
    private Expression offset;

    public ReceiveSortMergePlanNode() {
        // No-op.
    }

    public ReceiveSortMergePlanNode(
        int id,
        int edgeId,
        List<QueryDataType> fieldTypes,
        List<Expression> expressions,
        List<Boolean> ascs,
        Expression fetch,
        Expression offset
    ) {
        super(id);

        this.edgeId = edgeId;
        this.fieldTypes = fieldTypes;
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
    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public boolean isSender() {
        return false;
    }

    @Override
    public void visit(PlanNodeVisitor visitor) {
        visitor.onReceiveSortMergeNode(this);
    }

    @Override
    public PlanNodeSchema getSchema0() {
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public void writeData0(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);
        SerializationUtil.writeList(fieldTypes, out);
        out.writeObject(expressions);
        out.writeObject(ascs);
        out.writeObject(fetch);
        out.writeObject(offset);
    }

    @Override
    public void readData0(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();
        fieldTypes = SerializationUtil.readList(in);
        expressions = in.readObject();
        ascs = in.readObject();
        fetch = in.readObject();
        offset = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, edgeId, expressions, ascs, fetch, offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReceiveSortMergePlanNode that = (ReceiveSortMergePlanNode) o;

        return id == that.id && edgeId == that.edgeId && expressions.equals(that.expressions) && ascs.equals(that.ascs)
                   && Objects.equals(fetch, that.fetch) && Objects.equals(offset, that.offset);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", edgeId=" + edgeId + ", expressions=" + expressions
            + ", ascs=" + ascs + ", fetch=" + fetch + ", offset=" + offset + '}';
    }
}
