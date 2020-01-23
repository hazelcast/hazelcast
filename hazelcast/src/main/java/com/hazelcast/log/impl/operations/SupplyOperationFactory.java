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

package com.hazelcast.log.impl.operations;

import com.hazelcast.log.impl.LogDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.io.IOException;
import java.util.function.Supplier;

public class SupplyOperationFactory implements OperationFactory {
    private Supplier supplier;
    private String name;

    public SupplyOperationFactory() {
    }

    public SupplyOperationFactory(String name, Supplier supplier) {
        this.name = name;
        this.supplier = supplier;
    }

    @Override
    public Operation createOperation() {
        return new SupplyOperation(name, supplier);
    }

    @Override
    public int getFactoryId() {
        return LogDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return LogDataSerializerHook.SUPPLY_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(supplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        supplier = in.readObject();
    }
}