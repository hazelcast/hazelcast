/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.executor.impl.RunnableAdapter;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.RunStatus;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.concurrent.Callable;

import static com.hazelcast.spi.RunStatus.OFFLOADED;

abstract class AbstractCallableTaskOperation extends Operation implements IdentifiedDataSerializable {

    protected String name;
    protected String uuid;
    protected transient Callable callable;
    private Data callableData;

    public AbstractCallableTaskOperation() {
    }

    public AbstractCallableTaskOperation(String name, String uuid, Data callableData) {
        this.name = name;
        this.uuid = uuid;
        this.callableData = callableData;
    }

    @Override
    public final void run() throws Exception {
        Callable callable = loadCallable();
        DistributedExecutorService service = getService();
        service.execute(name, uuid, callable, this);
    }

    private Callable loadCallable() {
        Callable callable;
        try {
            callable = getNodeEngine().toObject(callableData);
        } catch (HazelcastSerializationException e) {
            sendResponse(e);
            throw ExceptionUtil.rethrow(e);
        }

        ManagedContext managedContext = getManagedContext();

        if (callable instanceof RunnableAdapter) {
            RunnableAdapter adapter = (RunnableAdapter) callable;
            Runnable runnable = (Runnable) managedContext.initialize(adapter.getRunnable());
            adapter.setRunnable(runnable);
        } else {
            callable = (Callable) managedContext.initialize(callable);
        }
        return callable;
    }

    private ManagedContext getManagedContext() {
        HazelcastInstanceImpl hazelcastInstance = (HazelcastInstanceImpl) getNodeEngine().getHazelcastInstance();
        SerializationService serializationService = hazelcastInstance.getSerializationService();
        return serializationService.getManagedContext();
    }

    @Override
    public RunStatus runStatus() {
        return OFFLOADED;
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
}
