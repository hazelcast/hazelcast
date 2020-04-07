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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.ZeroInputPlanNode;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical node which receives from remote stripes. The node carries the schema (field types).
 */
public class ReceivePlanNode extends ZeroInputPlanNode implements EdgeAwarePlanNode, IdentifiedDataSerializable {
    /** Edge ID. */
    private int edgeId;

    /** Field types. */
    private List<QueryDataType> fieldTypes;

    public ReceivePlanNode() {
        // No-op.
    }

    public ReceivePlanNode(int id, int edgeId, List<QueryDataType> fieldTypes) {
        super(id);

        this.edgeId = edgeId;
        this.fieldTypes = fieldTypes;
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
        visitor.onReceiveNode(this);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.NODE_RECEIVE;
    }

    @Override
    public PlanNodeSchema getSchema0() {
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public void writeData0(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);
        SerializationUtil.writeList(fieldTypes, out);
    }

    @Override
    public void readData0(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();
        fieldTypes = SerializationUtil.readList(in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, edgeId, fieldTypes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReceivePlanNode that = (ReceivePlanNode) o;

        return id == that.id && edgeId == that.edgeId && fieldTypes.equals(that.fieldTypes);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", edgeId=" + edgeId + ", fieldTypes=" + fieldTypes + '}';
    }
}
