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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class GetAndSetOperation extends AtomicReferenceBackupAwareOperation {

    private Data newValue;
    private Data returnValue;

    public GetAndSetOperation() {
        super();
    }

    public GetAndSetOperation(String name, Data newValue) {
        super(name);
        this.newValue = newValue;
    }

    @Override
    public void run() throws Exception {
        returnValue = getReference().getAndSet(newValue);
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(newValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        newValue = in.readObject();
    }

    public Operation getBackupOperation() {
        return new SetBackupOperation(name, newValue);
    }
}

