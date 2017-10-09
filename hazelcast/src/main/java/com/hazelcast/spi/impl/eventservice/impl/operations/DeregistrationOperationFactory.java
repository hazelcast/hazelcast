/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.eventservice.impl.operations;

import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.eventservice.impl.Registration;

import java.io.IOException;

/**
 * Factory that creates {@link DeregistrationOperation}s for a listener registration.
 */
public class DeregistrationOperationFactory implements OperationFactory {
    private final Registration registration;
    private final ClusterService clusterService;

    public DeregistrationOperationFactory() {
        registration = null;
        clusterService = null;
    }

    public DeregistrationOperationFactory(Registration reg, ClusterService clusterService) {
        registration = reg;
        this.clusterService = clusterService;
    }

    @Override
    public Operation createOperation() {
        return new DeregistrationOperation(registration.getTopic(), registration.getId(), clusterService.getMemberListVersion())
                .setServiceName(registration.getServiceName());
    }

    @Override
    public int getFactoryId() {
        throw new UnsupportedOperationException("DeregistrationOperationFactory must not be serialized");
    }

    @Override
    public int getId() {
        throw new UnsupportedOperationException("DeregistrationOperationFactory must not be serialized");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException("DeregistrationOperationFactory must not be serialized");
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException("DeregistrationOperationFactory must not be serialized");
    }
}
