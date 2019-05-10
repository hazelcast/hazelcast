/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.executor.impl.operations;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.ExecutorDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.CallStatus;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.Offload;
import com.hazelcast.spi.Operation;

import java.io.IOException;

abstract class AbstractCallableTaskOperation extends Operation implements NamedOperation, IdentifiedDataSerializable {

    protected String name;
    protected String uuid;
    private Data callableData;

    AbstractCallableTaskOperation() {
    }

    AbstractCallableTaskOperation(String name, String uuid, Data callableData) {
        this.name = name;
        this.uuid = uuid;
        this.callableData = callableData;
    }

    @Override
    public final CallStatus call() {
        return new OffloadImpl();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(uuid);
        out.writeData(callableData);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        uuid = in.readUTF();
        callableData = in.readData();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(name);
    }

    @Override
    public int getFactoryId() {
        return ExecutorDataSerializerHook.F_ID;
    }

    private class OffloadImpl extends Offload {
        OffloadImpl() {
            super(AbstractCallableTaskOperation.this);
        }

        @Override
        public void start() {
            DistributedExecutorService service = getService();
            service.execute(name, uuid, loadTask(), AbstractCallableTaskOperation.this);
        }

        private <T> T loadTask() {
            ManagedContext managedContext = serializationService.getManagedContext();

            Object object = serializationService.toObject(callableData);
            managedContext.initialize(object);
            return (T) object;
        }
    }
}
