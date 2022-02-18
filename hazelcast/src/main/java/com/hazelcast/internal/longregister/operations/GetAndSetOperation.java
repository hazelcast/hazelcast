/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.longregister.operations;

import com.hazelcast.internal.longregister.LongRegister;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;

import static com.hazelcast.internal.longregister.LongRegisterDataSerializerHook.GET_AND_SET;

public class GetAndSetOperation extends LongRegisterBackupAwareOperation implements MutatingOperation {

    private long newValue;
    private long returnValue;

    public GetAndSetOperation() {
    }

    public GetAndSetOperation(String name, long newValue) {
        super(name);
        this.newValue = newValue;
    }

    @Override
    public void run() throws Exception {
        LongRegister container = getLongContainer();
        returnValue = container.getAndSet(newValue);
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    public Operation getBackupOperation() {
        return new SetBackupOperation(name, newValue);
    }

    @Override
    public int getClassId() {
        return GET_AND_SET;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(newValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        newValue = in.readLong();
    }
}
