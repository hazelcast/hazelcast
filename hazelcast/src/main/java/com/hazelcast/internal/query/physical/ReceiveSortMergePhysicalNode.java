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

package com.hazelcast.internal.query.physical;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.physical.PhysicalNodeVisitor;

import java.io.IOException;
import java.util.List;

public class ReceiveSortMergePhysicalNode implements PhysicalNode {

    private List<Expression> expressions;
    private List<Boolean> ascs;
    private int edgeId;

    public ReceiveSortMergePhysicalNode() {
        // No-op.
    }

    public ReceiveSortMergePhysicalNode(List<Expression> expressions, List<Boolean> ascs, int edgeId) {
        this.expressions = expressions;
        this.ascs = ascs;
        this.edgeId = edgeId;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    public List<Boolean> getAscs() {
        return ascs;
    }

    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        visitor.onReceiveSortMergeNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(expressions);
        out.writeObject(ascs);
        out.writeInt(edgeId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        expressions = in.readObject();
        ascs = in.readObject();
        edgeId = in.readInt();
    }
}
