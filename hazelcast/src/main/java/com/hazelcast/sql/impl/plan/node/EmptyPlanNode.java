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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;

public class EmptyPlanNode extends AbstractPlanNode implements IdentifiedDataSerializable {

    private List<QueryDataType> fieldTypes;

    public EmptyPlanNode() {
        // No-op.
    }

    public EmptyPlanNode(int id, List<QueryDataType> fieldTypes) {
        super(id);

        this.fieldTypes = fieldTypes;
    }

    @Override
    protected PlanNodeSchema getSchema0() {
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.NODE_EMPTY;
    }

    @Override
    protected void writeData0(ObjectDataOutput out) throws IOException {
        SerializationUtil.writeList(fieldTypes, out);
    }

    @Override
    protected void readData0(ObjectDataInput in) throws IOException {
        fieldTypes = SerializationUtil.readList(in);
    }

    @Override
    public void visit(PlanNodeVisitor visitor) {
        visitor.onEmptyNode(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EmptyPlanNode planNode = (EmptyPlanNode) o;

        return id == planNode.id && fieldTypes.equals(planNode.fieldTypes);
    }

    @Override
    public int hashCode() {
        return 31 * id + fieldTypes.hashCode();
    }
}
