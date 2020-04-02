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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.UniInputPlanNode;
import com.hazelcast.sql.impl.partitioner.RowPartitioner;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;

import java.io.IOException;
import java.util.Objects;

/**
 * Node which unicasts data to remote stripes.
 */
public class UnicastSendPlanNode extends UniInputPlanNode implements EdgeAwarePlanNode {
    /** Edge ID. */
    private int edgeId;

    /** Partition hasher (get partition hash from row). */
    private RowPartitioner partitioner;

    public UnicastSendPlanNode() {
        // No-op.
    }

    public UnicastSendPlanNode(int id, PlanNode upstream, int edgeId, RowPartitioner partitioner) {
        super(id, upstream);

        this.edgeId = edgeId;
        this.partitioner = partitioner;
    }

    public RowPartitioner getPartitioner() {
        return partitioner;
    }

    @Override
    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public boolean isSender() {
        return true;
    }

    @Override
    public void visit0(PlanNodeVisitor visitor) {
        visitor.onUnicastSendNode(this);
    }

    @Override
    public void writeData1(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);
        out.writeObject(partitioner);
    }

    @Override
    public void readData1(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();
        partitioner = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, edgeId, upstream, partitioner);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnicastSendPlanNode that = (UnicastSendPlanNode) o;

        return id == that.id && edgeId == that.edgeId && upstream.equals(that.upstream) && partitioner.equals(that.partitioner);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", edgeId=" + edgeId + ", partitioner=" + partitioner
            + ", upstream=" + upstream + '}';
    }
}
