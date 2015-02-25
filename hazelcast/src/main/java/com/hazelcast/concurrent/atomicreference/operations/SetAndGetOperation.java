/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomicreference.operations;

import com.hazelcast.concurrent.atomicreference.AtomicReferenceDataSerializerHook;
import com.hazelcast.concurrent.atomicreference.ReferenceWrapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class SetAndGetOperation extends AtomicReferenceBackupAwareOperation {

    private Data newValue;
    private Data returnValue;

    public SetAndGetOperation() {
    }

    public SetAndGetOperation(String name, Data newValue) {
        super(name);
        this.newValue = newValue;
    }

    @Override
    public void run() throws Exception {
        ReferenceWrapper reference = getReference();
        reference.getAndSet(newValue);
        returnValue = newValue;
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    public int getId() {
        return AtomicReferenceDataSerializerHook.SET_AND_GET;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(newValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        newValue = in.readData();
    }

    @Override
    public Operation getBackupOperation() {
        return new SetBackupOperation(name, newValue);
    }
}
