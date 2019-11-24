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
 * Filter node.
 */
public class FilterPhysicalNode extends UniInputPhysicalNode {
    /** Condition. */
    private Expression<Boolean> filter;

    public FilterPhysicalNode() {
        // No-op.
    }

    public FilterPhysicalNode(PhysicalNode upstream, Expression<Boolean> filter) {
        super(upstream);

        assert filter != null;

        this.filter = filter;
    }

    public Expression<Boolean> getFilter() {
        return filter;
    }

    @Override
    public void visit0(PhysicalNodeVisitor visitor) {
        visitor.onFilterNode(this);
    }

    @Override
    public void writeData0(ObjectDataOutput out) throws IOException {
        out.writeObject(filter);
    }

    @Override
    public void readData0(ObjectDataInput in) throws IOException {
        filter = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(upstream, filter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FilterPhysicalNode that = (FilterPhysicalNode) o;

        return upstream.equals(that.upstream) && filter.equals(that.filter);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{filter=" + filter + ", upstream=" + upstream + '}';
    }
}
