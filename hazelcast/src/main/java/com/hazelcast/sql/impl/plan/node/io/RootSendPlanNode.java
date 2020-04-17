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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.UniInputPlanNode;

import java.io.IOException;
import java.util.Objects;

/**
 * Node that sends data to the root fragment.
 */
public class RootSendPlanNode extends UniInputPlanNode implements EdgeAwarePlanNode, IdentifiedDataSerializable {
    /** Edge ID. */
    private int edgeId;

    public RootSendPlanNode() {
        // No-op.
    }

    public RootSendPlanNode(int id, PlanNode upstream, int edgeId) {
        super(id, upstream);

        this.edgeId = edgeId;
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
        visitor.onRootSendNode(this);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.NODE_ROOT_SEND;
    }

    @Override
    public void writeData1(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);
    }

    @Override
    public void readData1(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, edgeId, upstream);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RootSendPlanNode that = (RootSendPlanNode) o;

        return id == that.id && edgeId == that.edgeId && upstream.equals(that.upstream);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", edgeId=" + edgeId + ", upstream=" + upstream + '}';
    }
}
