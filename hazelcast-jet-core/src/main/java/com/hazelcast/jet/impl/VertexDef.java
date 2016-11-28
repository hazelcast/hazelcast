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

import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.Util.readList;
import static com.hazelcast.jet.impl.Util.writeList;

class VertexDef implements IdentifiedDataSerializable {

    private int id;
    private List<EdgeDef> inputs = new ArrayList<>();
    private List<EdgeDef> outputs = new ArrayList<>();
    private ProcessorSupplier processorSupplier;
    private int parallelism;

    VertexDef() {
    }

    VertexDef(int id, ProcessorSupplier processorSupplier, int parallelism) {
        this.id = id;
        this.processorSupplier = processorSupplier;
        this.parallelism = parallelism;
    }

    int getVertexId() {
        return id;
    }

    void addInputs(List<EdgeDef> inputs) {
        this.inputs.addAll(inputs);
    }

    void addOutputs(List<EdgeDef> outputs) {
        this.outputs.addAll(outputs);
    }

    List<EdgeDef> getInputs() {
        return inputs;
    }

    List<EdgeDef> getOutputs() {
        return outputs;
    }

    ProcessorSupplier getProcessorSupplier() {
        return processorSupplier;
    }

    int getParallelism() {
        return parallelism;
    }

    @Override
    public int getFactoryId() {
        return JetImplDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetImplDataSerializerHook.VERTEX_DEF;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(id);
        writeList(out, inputs);
        writeList(out, outputs);
        out.writeObject(processorSupplier);
        out.writeInt(parallelism);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readInt();
        inputs = readList(in);
        outputs = readList(in);
        processorSupplier = in.readObject();
        parallelism = in.readInt();
    }
}
