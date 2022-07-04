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

package com.hazelcast.executor.impl.operations;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.ExecutorDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.NamedOperation;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.UUID;

abstract class AbstractCallableTaskOperation extends Operation implements NamedOperation, IdentifiedDataSerializable {

    protected String name;
    protected UUID uuid;
    private Data callableData;

    AbstractCallableTaskOperation() {
    }

    AbstractCallableTaskOperation(String name,
                                  UUID uuid,
                                  @Nonnull Data callableData) {
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
        out.writeString(name);
        UUIDSerializationUtil.writeUUID(out, uuid);
        IOUtil.writeData(out, callableData);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
        uuid = UUIDSerializationUtil.readUUID(in);
        callableData = IOUtil.readData(in);
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
            return (T) managedContext.initialize(object);
        }
    }
}
