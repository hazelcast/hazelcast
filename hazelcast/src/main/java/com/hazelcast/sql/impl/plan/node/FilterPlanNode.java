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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;

import java.io.IOException;
import java.util.Objects;

/**
 * Filter node.
 */
public class FilterPlanNode extends UniInputPlanNode implements IdentifiedDataSerializable {

    private Expression<Boolean> filter;

    public FilterPlanNode() {
        // No-op.
    }

    public FilterPlanNode(int id, PlanNode upstream, Expression<Boolean> filter) {
        super(id, upstream);

        assert filter != null;

        this.filter = filter;
    }

    public Expression<Boolean> getFilter() {
        return filter;
    }

    @Override
    public void visit0(PlanNodeVisitor visitor) {
        visitor.onFilterNode(this);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.NODE_FILTER;
    }

    @Override
    public void writeData1(ObjectDataOutput out) throws IOException {
        out.writeObject(filter);
    }

    @Override
    public void readData1(ObjectDataInput in) throws IOException {
        filter = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, filter, upstream);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FilterPlanNode that = (FilterPlanNode) o;

        return id == that.id && filter.equals(that.filter) && upstream.equals(that.upstream);
    }
}
