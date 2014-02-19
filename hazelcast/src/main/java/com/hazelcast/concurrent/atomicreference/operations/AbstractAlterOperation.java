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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public abstract class AbstractAlterOperation extends AtomicReferenceBackupAwareOperation {

    protected Data function;
    protected Object response;
    protected Data backup;

    public AbstractAlterOperation() {
    }

    public AbstractAlterOperation(String name, Data function) {
        super(name);
        this.function = function;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    protected boolean isEquals(Object o1, Object o2) {
        if (o1 == null) {
            return o2 == null;
        }

        if (o1 == o2) {
            return true;
        }

        return o1.equals(o2);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(function);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        function = in.readObject();
    }

    @Override
    public Operation getBackupOperation() {
        return new SetBackupOperation(name, backup);
    }
}
