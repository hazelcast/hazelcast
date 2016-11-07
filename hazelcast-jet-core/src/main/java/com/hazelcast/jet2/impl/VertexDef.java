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

import com.hazelcast.jet2.ProcessorSupplier;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class VertexDef implements Serializable {

    private int id;
    private final List<EdgeDef> inputs = new ArrayList<>();
    private final List<EdgeDef> outputs = new ArrayList<>();
    private final ProcessorSupplier processorSupplier;
    private final int parallelism;

    public VertexDef(int id, ProcessorSupplier processorSupplier, int parallelism) {
        this.id = id;
        this.processorSupplier = processorSupplier;
        this.parallelism = parallelism;
    }

    public int getId() {
        return id;
    }

    public void addInputs(List<EdgeDef> inputs) {
        this.inputs.addAll(inputs);
    }

    public void addOutputs(List<EdgeDef> outputs) {
        this.outputs.addAll(outputs);
    }

    public List<EdgeDef> getInputs() {
        return inputs;
    }

    public List<EdgeDef> getOutputs() {
        return outputs;
    }

    public ProcessorSupplier getProcessorSupplier() {
        return processorSupplier;
    }

    public int getParallelism() {
        return parallelism;
    }
}
