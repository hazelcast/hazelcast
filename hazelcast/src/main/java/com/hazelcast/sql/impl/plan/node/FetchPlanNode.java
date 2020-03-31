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
import com.hazelcast.sql.impl.expression.Expression;

import java.io.IOException;
import java.util.Objects;

@SuppressWarnings("rawtypes")
public class FetchPlanNode extends UniInputPlanNode {
    /** Limit. */
    private Expression fetch;

    /** Offset. */
    private Expression offset;

    public FetchPlanNode() {
        // No-op.
    }

    public FetchPlanNode(int id, PlanNode upstream, Expression fetch, Expression offset) {
        super(id, upstream);

        this.fetch = fetch;
        this.offset = offset;
    }

    public Expression getFetch() {
        return fetch;
    }

    public Expression getOffset() {
        return offset;
    }

    @Override
    public void visit0(PlanNodeVisitor visitor) {
        visitor.onFetchNode(this);
    }

    @Override
    public void writeData1(ObjectDataOutput out) throws IOException {
        out.writeObject(fetch);
        out.writeObject(offset);
    }

    @Override
    public void readData1(ObjectDataInput in) throws IOException {
        fetch = in.readObject();
        offset = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, fetch, offset, upstream);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FetchPlanNode that = (FetchPlanNode) o;

        return id == that.id && Objects.equals(fetch, that.fetch) && Objects.equals(offset, that.offset)
                   && upstream.equals(that.upstream);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", fetch=" + fetch + ", offset=" + offset
                   + ", upstream=" + upstream + '}';
    }
}
