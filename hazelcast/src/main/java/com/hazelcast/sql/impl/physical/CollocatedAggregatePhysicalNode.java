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

package com.hazelcast.sql.impl.physical;

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.aggregate.AggregateExpression;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Collocated aggregation.
 */
public class CollocatedAggregatePhysicalNode implements PhysicalNode {
    /** Upstream node. */
    private PhysicalNode upstream;

    /** Group key size, i.e. how many columns are participating in the group key. */
    private int groupKeySize;

    /** Accumulators. */
    private List<AggregateExpression> accumulators;

    /** Whether group key is already sorted, and hence blocking behavior is not needed. */
    private boolean sorted;

    public CollocatedAggregatePhysicalNode() {
        // No-op.
    }

    public CollocatedAggregatePhysicalNode(
        PhysicalNode upstream,
        int groupKeySize,
        List<AggregateExpression> accumulators,
        boolean sorted
    ) {
        this.upstream = upstream;
        this.groupKeySize = groupKeySize;
        this.accumulators = accumulators;
        this.sorted = sorted;
    }

    public PhysicalNode getUpstream() {
        return upstream;
    }

    public int getGroupKeySize() {
        return groupKeySize;
    }

    public List<AggregateExpression> getAccumulators() {
        return accumulators;
    }

    public boolean isSorted() {
        return sorted;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        upstream.visit(visitor);

        visitor.onCollocatedAggregateNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(upstream);
        out.writeInt(groupKeySize);
        SerializationUtil.writeList(accumulators, out);
        out.writeBoolean(sorted);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        upstream = in.readObject();
        groupKeySize = in.readInt();
        accumulators = SerializationUtil.readList(in);
        sorted = in.readBoolean();
    }

    @Override
    public int hashCode() {
        return Objects.hash(upstream, groupKeySize, accumulators, sorted);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CollocatedAggregatePhysicalNode that = (CollocatedAggregatePhysicalNode) o;

        return groupKeySize == that.groupKeySize && sorted == that.sorted && upstream.equals(that.upstream)
            && accumulators.equals(that.accumulators);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{accumulators=" + accumulators + ", groupKeySize=" + groupKeySize
            + ", sorted=" + sorted + ", upstream=" + upstream + '}';
    }
}
