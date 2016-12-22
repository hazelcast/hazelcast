/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.Edge;
import com.hazelcast.jet.EdgeConfig;
import com.hazelcast.jet.Partitioner;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

class EdgeDef implements IdentifiedDataSerializable {

    private int oppositeVertexId;
    private int oppositeEndOrdinal;
    private int ordinal;
    private int priority;
    private boolean isDistributed;
    private Edge.ForwardingPattern forwardingPattern;
    private Partitioner partitioner;
    private EdgeConfig config;

    EdgeDef() {
    }

    EdgeDef(int oppositeVertexId, int ordinal, int oppositeEndOrdinal, int priority,
            boolean isDistributed, Edge.ForwardingPattern forwardingPattern, Partitioner partitioner,
            EdgeConfig config) {
        this.oppositeVertexId = oppositeVertexId;
        this.oppositeEndOrdinal = oppositeEndOrdinal;
        this.ordinal = ordinal;
        this.priority = priority;
        this.isDistributed = isDistributed;
        this.forwardingPattern = forwardingPattern;
        this.partitioner = partitioner;
        this.config = config;
    }

    int getOrdinal() {
        return ordinal;
    }

    int getOppositeEndOrdinal() {
        return oppositeEndOrdinal;
    }

    int getOppositeVertexId() {
        return oppositeVertexId;
    }

    Edge.ForwardingPattern getForwardingPattern() {
        return forwardingPattern;
    }

    Partitioner getPartitioner() {
        return partitioner;
    }

    int getPriority() {
        return priority;
    }

    boolean isDistributed() {
        return isDistributed;
    }

    public EdgeConfig getConfig() {
        return config;
    }
    // IdentifiedDataSerializable implementation

    @Override
    public int getFactoryId() {
        return JetImplDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetImplDataSerializerHook.EDGE_DEF;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(oppositeVertexId);
        out.writeInt(oppositeEndOrdinal);
        out.writeInt(ordinal);
        out.writeInt(priority);
        out.writeBoolean(isDistributed);
        out.writeObject(forwardingPattern);
        CustomClassLoadedObject.write(out, partitioner);
        out.writeObject(config);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        oppositeVertexId = in.readInt();
        oppositeEndOrdinal = in.readInt();
        ordinal = in.readInt();
        priority = in.readInt();
        isDistributed = in.readBoolean();
        forwardingPattern = in.readObject();
        partitioner = CustomClassLoadedObject.read(in);
        config = in.readObject();
    }
}
