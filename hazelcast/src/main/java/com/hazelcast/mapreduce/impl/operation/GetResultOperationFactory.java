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

package com.hazelcast.mapreduce.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

/**
 * This class is used to create {@link com.hazelcast.mapreduce.impl.operation.GetResultOperation}
 * instances.<br/>
 * In difference to other implementations of {@link com.hazelcast.spi.OperationFactory} this class
 * is never ever serialized and the DataSerializable methods {@link #readData(com.hazelcast.nio.ObjectDataInput)}
 * and {@link #writeData(com.hazelcast.nio.ObjectDataOutput)} throw {@link java.lang.UnsupportedOperationException}s.
 */
public class GetResultOperationFactory
        implements OperationFactory {

    private final String name;
    private final String jobId;

    public GetResultOperationFactory(String name, String jobId) {
        this.name = name;
        this.jobId = jobId;
    }

    @Override
    public Operation createOperation() {
        return new GetResultOperation(name, jobId);
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        throw new UnsupportedOperationException("local factory only");
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        throw new UnsupportedOperationException("local factory only");
    }

}
