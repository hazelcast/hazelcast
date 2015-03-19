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

package com.hazelcast.concurrent.atomicreference.impl.operations;

import com.hazelcast.concurrent.atomicreference.impl.AtomicReferenceDataSerializerHook;
import com.hazelcast.concurrent.atomicreference.impl.AtomicReferenceService;
import com.hazelcast.concurrent.atomicreference.impl.ReferenceContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public abstract class AtomicReferenceBaseOperation extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    protected String name;

    public AtomicReferenceBaseOperation() {
    }

    public AtomicReferenceBaseOperation(String name) {
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return AtomicReferenceService.SERVICE_NAME;
    }

    public ReferenceContainer getReferenceContainer() {
        AtomicReferenceService service = getService();
        return service.getReferenceContainer(name);
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public int getFactoryId() {
        return AtomicReferenceDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }
}
