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

package com.hazelcast.sql.impl.physical.io;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.physical.PhysicalNodeVisitor;
import com.hazelcast.sql.impl.physical.ZeroInputPhysicalNode;

import java.io.IOException;
import java.util.Objects;

/**
 * Physical node which receives from remote stripes.
 */
public class ReceivePhysicalNode extends ZeroInputPhysicalNode implements EdgeAwarePhysicalNode {
    /** Edge ID. */
    private int edgeId;

    public ReceivePhysicalNode() {
        // No-op.
    }

    public ReceivePhysicalNode(int edgeId) {
        this.edgeId = edgeId;
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
    public void visit(PhysicalNodeVisitor visitor) {
        visitor.onReceiveNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReceivePhysicalNode that = (ReceivePhysicalNode) o;

        return edgeId == that.edgeId;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{edgeId=" + edgeId + '}';
    }
}
