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
import java.util.List;

/**
 * Projection.
 */
public class ProjectPhysicalNode implements PhysicalNode {
    /** Upstream node. */
    private PhysicalNode upstream;

    /** Projections. */
    private List<Expression> projections;

    public ProjectPhysicalNode() {
        // No-op.
    }

    public ProjectPhysicalNode(PhysicalNode upstream, List<Expression> projections) {
        this.upstream = upstream;
        this.projections = projections;
    }

    public PhysicalNode getUpstream() {
        return upstream;
    }

    public List<Expression> getProjections() {
        return projections;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        upstream.visit(visitor);

        visitor.onProjectNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(upstream);
        out.writeObject(projections);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        upstream = in.readObject();
        projections = in.readObject();
    }
}
