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
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Projection.
 */
@SuppressWarnings("rawtypes")
public class ProjectPlanNode extends UniInputPlanNode implements IdentifiedDataSerializable {

    private List<Expression> projects;

    public ProjectPlanNode() {
        // No-op.
    }

    public ProjectPlanNode(int id, PlanNode upstream, List<Expression> projects) {
        super(id, upstream);

        this.projects = projects;
    }

    public List<Expression> getProjects() {
        return projects;
    }

    @Override
    public void visit0(PlanNodeVisitor visitor) {
        visitor.onProjectNode(this);
    }

    @Override
    public PlanNodeSchema getSchema0() {
        List<QueryDataType> types = new ArrayList<>(projects.size());

        for (Expression project : projects) {
            types.add(project.getType());
        }

        return new PlanNodeSchema(types);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.NODE_PROJECT;
    }

    @Override
    public void writeData1(ObjectDataOutput out) throws IOException {
        SerializationUtil.writeList(projects, out);
    }

    @Override
    public void readData1(ObjectDataInput in) throws IOException {
        projects = SerializationUtil.readList(in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, projects, upstream);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProjectPlanNode that = (ProjectPlanNode) o;

        return id == that.id && projects.equals(that.projects) && upstream.equals(that.upstream);
    }
}
