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

package com.hazelcast.sql.impl.physical;

import com.hazelcast.sql.impl.physical.visitor.PhysicalNodeVisitor;

import java.util.Objects;

/**
 * In-memory table.
 */
public class MaterializedInputPhysicalNode extends UniInputPhysicalNode {
    public MaterializedInputPhysicalNode() {
        // No-op.
    }

    public MaterializedInputPhysicalNode(int id, PhysicalNode upstream) {
        super(id, upstream);
    }

    @Override
    public void visit0(PhysicalNodeVisitor visitor) {
        visitor.onMaterializedInputNode(this);
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

        MaterializedInputPhysicalNode that = (MaterializedInputPhysicalNode) o;

        return id == that.id && upstream.equals(that.upstream);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", upstream=" + upstream + '}';
    }
}
