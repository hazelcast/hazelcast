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
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.physical.PhysicalNodeVisitor;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical node which receives from remote stripes and performs sort-merge.
 */
public class ReceiveSortMergePhysicalNode implements EdgeAwarePhysicalNode {
    /** Edge iD. */
    private int edgeId;

    /** Expressions to be used for sorting. */
    private List<Expression> expressions;

    /** Sort directions. */
    private List<Boolean> ascs;

    public ReceiveSortMergePhysicalNode() {
        // No-op.
    }

    public ReceiveSortMergePhysicalNode(int edgeId, List<Expression> expressions, List<Boolean> ascs) {
        this.edgeId = edgeId;
        this.expressions = expressions;
        this.ascs = ascs;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    public List<Boolean> getAscs() {
        return ascs;
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
        visitor.onReceiveSortMergeNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);
        out.writeObject(expressions);
        out.writeObject(ascs);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();
        expressions = in.readObject();
        ascs = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeId, expressions, ascs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReceiveSortMergePhysicalNode that = (ReceiveSortMergePhysicalNode) o;

        return edgeId == that.edgeId && expressions.equals(that.expressions) && ascs.equals(that.ascs);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{edgeId=" + edgeId + ", expressions=" + expressions
            + ", ascs=" + ascs + '}';
    }
}
