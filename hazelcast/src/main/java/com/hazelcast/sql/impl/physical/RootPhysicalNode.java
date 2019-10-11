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

import java.io.IOException;
import java.util.Objects;

/**
 * Root physical node. Performs final collection of results.
 */
public class RootPhysicalNode implements PhysicalNode {
    /** Upstream node. */
    private PhysicalNode upstream;

    public RootPhysicalNode() {
        // No-op.
    }

    public RootPhysicalNode(PhysicalNode upstream) {
        this.upstream = upstream;
    }

    public PhysicalNode getUpstream() {
        return upstream;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        upstream.visit(visitor);

        visitor.onRootNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(upstream);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        upstream = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(upstream);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RootPhysicalNode that = (RootPhysicalNode) o;

        return upstream.equals(that.upstream);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{upstream=" + upstream + '}';
    }
}
