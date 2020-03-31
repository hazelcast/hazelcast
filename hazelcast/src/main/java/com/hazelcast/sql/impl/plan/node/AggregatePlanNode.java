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

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.aggregate.AggregateExpression;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Collocated aggregation.
 */
public class AggregatePlanNode extends UniInputPlanNode {
    /** Group key. */
    private List<Integer> groupKey;

    /** Accumulators. */
    private List<AggregateExpression> expressions;

    /** Whether group key is already sorted, and hence blocking behavior is not needed. */
    private int sortedGroupKeySize;

    public AggregatePlanNode() {
        // No-op.
    }

    public AggregatePlanNode(
        int id,
        PlanNode upstream,
        List<Integer> groupKey,
        List<AggregateExpression> expressions,
        int sortedGroupKeySize
    ) {
        super(id, upstream);

        this.groupKey = groupKey;
        this.expressions = expressions;
        this.sortedGroupKeySize = sortedGroupKeySize;
    }

    public PlanNode getUpstream() {
        return upstream;
    }

    public List<Integer> getGroupKey() {
        return groupKey;
    }

    public List<AggregateExpression> getExpressions() {
        return expressions;
    }

    public int getSortedGroupKeySize() {
        return sortedGroupKeySize;
    }

    @Override
    public void visit0(PlanNodeVisitor visitor) {
        visitor.onAggregateNode(this);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public PlanNodeSchema getSchema0() {
        List<QueryDataType> types = new ArrayList<>(groupKey.size() + expressions.size());

        PlanNodeSchema upstreamSchema = upstream.getSchema();

        for (int groupKeyItem : groupKey) {
            types.add(upstreamSchema.getType(groupKeyItem));
        }

        for (AggregateExpression expression : expressions) {
            types.add(expression.getType());
        }

        return new PlanNodeSchema(types);
    }

    @Override
    public void writeData1(ObjectDataOutput out) throws IOException {
        SerializationUtil.writeList(groupKey, out);
        SerializationUtil.writeList(expressions, out);
        out.writeInt(sortedGroupKeySize);
    }

    @Override
    public void readData1(ObjectDataInput in) throws IOException {
        groupKey = SerializationUtil.readList(in);
        expressions = SerializationUtil.readList(in);
        sortedGroupKeySize = in.readInt();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, upstream, groupKey, expressions, sortedGroupKeySize);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AggregatePlanNode that = (AggregatePlanNode) o;

        return id == that.id && upstream.equals(that.upstream) && Objects.equals(groupKey, that.groupKey)
            && expressions.equals(that.expressions) && sortedGroupKeySize == that.sortedGroupKeySize ;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", groupKey=" + groupKey + ", expressions=" + expressions
            + ", sortedGroupKeySize=" + sortedGroupKeySize + ", upstream=" + upstream + '}';
    }
}
