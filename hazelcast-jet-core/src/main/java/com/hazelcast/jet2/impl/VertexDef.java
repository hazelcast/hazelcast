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

package com.hazelcast.jet2.impl;

import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.ProcessorSupplier;
import com.hazelcast.nio.Address;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class ExecutionPlan implements Serializable {
    private List<VertexDef> vertices = new ArrayList<>();
}

class VertexDef implements Serializable {

    private VertexId id;
    private final List<InputDef> inputs = new ArrayList<>();
    private final List<OutputDef> outputs = new ArrayList<>();
    private ProcessorSupplier processorSupplier;

    public VertexDef() {

    }

}

class InputDef implements Serializable {
    private final VertexId vertexId;
    private final int ordinal;
    private final Edge edge;

    public InputDef(VertexId vertexId, int ordinal, Edge edge) {
        this.vertexId = vertexId;
        this.ordinal = ordinal;
        this.edge = edge;
    }
}

class OutputDef implements Serializable {
    private final VertexId vertexId;
    private final int ordinal;
    private final Edge edge;

    public OutputDef(VertexId vertexId, int ordinal, Edge edge) {
        this.vertexId = vertexId;
        this.ordinal = ordinal;
        this.edge = edge;
    }
}

class VertexId implements Serializable {
    private final String name;
    private final Address address;

    public VertexId(String name, Address address) {
        this.name = name;
        this.address = address;
    }
}


