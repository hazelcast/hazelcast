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

package com.hazelcast.concurrent.atomicreference.operations;

import com.hazelcast.concurrent.atomicreference.AtomicReferenceDataSerializerHook;
import com.hazelcast.concurrent.atomicreference.ReferenceWrapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class CompareAndSetOperation extends AtomicReferenceBackupAwareOperation {

    private Data expect;
    private Data update;
    private boolean returnValue;

    public CompareAndSetOperation() {
    }

    public CompareAndSetOperation(String name, Data expect, Data update) {
        super(name);
        this.expect = expect;
        this.update = update;
    }

    @Override
    public void run() throws Exception {
        ReferenceWrapper reference = getReference();
        returnValue = reference.compareAndSet(expect, update);
        shouldBackup = !returnValue;
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    public int getId() {
        return AtomicReferenceDataSerializerHook.COMPARE_AND_SET;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(expect);
        out.writeObject(update);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expect = in.readObject();
        update = in.readObject();
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public Operation getBackupOperation() {
        return new SetBackupOperation(name, update);
    }
}

