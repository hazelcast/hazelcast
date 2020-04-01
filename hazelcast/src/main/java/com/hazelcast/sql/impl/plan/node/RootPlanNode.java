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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.util.Objects;

/**
 * Root node. Collects the final results of a query and exposes them to the user.
 */
public class RootPlanNode extends UniInputPlanNode implements IdentifiedDataSerializable {
    public RootPlanNode() {
        // No-op.
    }

    public RootPlanNode(int id, PlanNode upstream) {
        super(id, upstream);
    }

    @Override
    public void visit0(PlanNodeVisitor visitor) {
        visitor.onRootNode(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, upstream);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RootPlanNode that = (RootPlanNode) o;

        return id == that.id && upstream.equals(that.upstream);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.NODE_ROOT;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", upstream=" + upstream + '}';
    }
}
