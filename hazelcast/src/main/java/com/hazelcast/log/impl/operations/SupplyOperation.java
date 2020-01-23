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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.log.impl.LogContainer;
import com.hazelcast.log.impl.LogDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.function.Supplier;

public class SupplyOperation extends LogOperation {

    private Supplier supplier;

    public SupplyOperation() {
    }

    public SupplyOperation(String name, Supplier supplier) {
        super(name);
        this.supplier = supplier;
    }

    @Override
    public void run() throws Exception {
        // we need to clone the supplier
        SerializationService ss = getNodeEngine().getSerializationService();
        Supplier supplier = ss.toObject(ss.toData(this.supplier));
        LogContainer container = getContainer();
        container.putMany(supplier);
    }

    @Override
    public int getClassId() {
        return LogDataSerializerHook.SUPPLY;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(supplier);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        supplier = in.readObject();
    }
}

