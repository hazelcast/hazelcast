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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

/**
 * Factory that creates {@link AddDynamicConfigOperation}s for a given config.
 */
public class AddDynamicConfigOperationFactory implements OperationFactory {

    private final ClusterService clusterService;
    private final IdentifiedDataSerializable config;

    public AddDynamicConfigOperationFactory() {
        this.clusterService = null;
        this.config = null;
    }

    public AddDynamicConfigOperationFactory(ClusterService clusterService, IdentifiedDataSerializable config) {
        this.clusterService = clusterService;
        this.config = config;
    }

    @Override
    public Operation createOperation() {
        return new AddDynamicConfigOperation(config, clusterService.getMemberListVersion());
    }

    @Override
    public int getFactoryId() {
        throw new UnsupportedOperationException("AddDynamicConfigOperationFactory must not be serialized");
    }

    @Override
    public int getId() {
        throw new UnsupportedOperationException("AddDynamicConfigOperationFactory must not be serialized");
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException("AddDynamicConfigOperationFactory must not be serialized");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException("AddDynamicConfigOperationFactory must not be serialized");
    }
}
