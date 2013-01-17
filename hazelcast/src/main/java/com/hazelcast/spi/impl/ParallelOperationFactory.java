/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.cluster.NodeAware;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.MultiPartitionOperationFactory;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @mdogan 1/17/13
 */
public final class ParallelOperationFactory implements MultiPartitionOperationFactory,
        NodeAware, IdentifiedDataSerializable {

    private Data operationData;
    private transient NodeEngine nodeEngine;

    public ParallelOperationFactory() {
    }

    public ParallelOperationFactory(Operation operation, NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.operationData = nodeEngine.toData(operation);
    }

    public ParallelOperationFactory(Data operationData) {
        this.operationData = operationData;
    }

    public boolean shouldRunParallel() {
        return true;
    }

    public Operation createSequentialOperation() {
        throw new UnsupportedOperationException();
    }

    public Operation createParallelOperation() {
        return nodeEngine.toObject(operationData);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        operationData.writeData(out);
    }

    public void readData(ObjectDataInput in) throws IOException {
        operationData = new Data();
        operationData.readData(in);
    }

    public void setNode(Node node) {
        this.nodeEngine = node.nodeEngine;
    }

    public int getId() {
        return DataSerializerSpiHook.PARALLEL_OPERATION_FACTORY;
    }
}
