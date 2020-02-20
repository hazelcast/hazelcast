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

package com.hazelcast.sql.impl.physical.io;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.physical.UniInputPhysicalNode;
import com.hazelcast.sql.impl.physical.hash.HashFunction;
import com.hazelcast.sql.impl.physical.visitor.PhysicalNodeVisitor;

import java.io.IOException;
import java.util.Objects;

/**
 * Node which unicasts data to remote stripes.
 */
public class UnicastSendPhysicalNode extends UniInputPhysicalNode implements EdgeAwarePhysicalNode {
    /** Edge ID. */
    private int edgeId;

    /** Partition hasher (get partition hash from row). */
    private HashFunction hashFunction;

    public UnicastSendPhysicalNode() {
        // No-op.
    }

    public UnicastSendPhysicalNode(int id, PhysicalNode upstream, int edgeId, HashFunction hashFunction) {
        super(id, upstream);

        this.edgeId = edgeId;
        this.hashFunction = hashFunction;
    }

    public HashFunction getHashFunction() {
        return hashFunction;
    }

    @Override
    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public boolean isSender() {
        return true;
    }

    @Override
    public void visit0(PhysicalNodeVisitor visitor) {
        visitor.onUnicastSendNode(this);
    }

    @Override
    public void writeData1(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);
        out.writeObject(hashFunction);
    }

    @Override
    public void readData1(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();
        hashFunction = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, edgeId, upstream, hashFunction);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnicastSendPhysicalNode that = (UnicastSendPhysicalNode) o;

        return id == that.id && edgeId == that.edgeId && upstream.equals(that.upstream) && hashFunction.equals(that.hashFunction);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + id + ", edgeId=" + edgeId + ", hashFunction=" + hashFunction
            + ", upstream=" + upstream + '}';
    }
}
