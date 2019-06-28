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

package com.hazelcast.internal.query.physical;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.physical.PhysicalNodeVisitor;

import java.io.IOException;

public class SendPhysicalNode implements PhysicalNode {
    /** Edge ID. */
    private int edgeId;

    /** Child node. */
    private PhysicalNode delegate;

    /** Partition hasher (get partition hash from row). */
    private Expression<Integer> partitionHasher;

    /** Whether data partitions should be used to resolve target. */
    // TODO: Somethins is wrong here! partitionHasher + useDataPartitions should be the same?
    private boolean useDataPartitions;

    public SendPhysicalNode() {
        // No-op.
    }

    public SendPhysicalNode(int edgeId, PhysicalNode delegate, Expression<Integer> partitionHasher,
        boolean useDataPartitions) {
        this.edgeId = edgeId;
        this.delegate = delegate;
        this.partitionHasher = partitionHasher;
        this.useDataPartitions = useDataPartitions;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public PhysicalNode getDelegate() {
        return delegate;
    }

    public Expression<Integer> getPartitionHasher() {
        return partitionHasher;
    }

    public boolean isUseDataPartitions() {
        return useDataPartitions;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        delegate.visit(visitor);

        visitor.onSendNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);
        out.writeObject(delegate);
        out.writeObject(partitionHasher);
        out.writeBoolean(useDataPartitions);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();
        delegate = in.readObject();
        partitionHasher = in.readObject();
        useDataPartitions = in.readBoolean();
    }
}
