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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;

import java.io.IOException;
import java.util.Objects;

/**
 * Node which sends data to remote stripes.
 */
public class SendPhysicalNode implements PhysicalNode {
    /** Edge ID. */
    private int edgeId;

    /** Upstream. */
    private PhysicalNode upstream;

    /** Partition hasher (get partition hash from row). */
    private Expression<Integer> partitionHasher;

    public SendPhysicalNode() {
        // No-op.
    }

    public SendPhysicalNode(int edgeId, PhysicalNode upstream, Expression<Integer> partitionHasher) {
        this.edgeId = edgeId;
        this.upstream = upstream;
        this.partitionHasher = partitionHasher;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public PhysicalNode getUpstream() {
        return upstream;
    }

    public Expression<Integer> getPartitionHasher() {
        return partitionHasher;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        upstream.visit(visitor);

        visitor.onSendNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);
        out.writeObject(upstream);
        out.writeObject(partitionHasher);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();
        upstream = in.readObject();
        partitionHasher = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeId, upstream, partitionHasher);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SendPhysicalNode that = (SendPhysicalNode) o;

        return edgeId == that.edgeId && upstream.equals(that.upstream)
            && Objects.equals(partitionHasher, that.partitionHasher);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{edgeId=" + edgeId + ", partitionHasher=" + partitionHasher
            + ", upstream=" + upstream + '}';
    }
}
