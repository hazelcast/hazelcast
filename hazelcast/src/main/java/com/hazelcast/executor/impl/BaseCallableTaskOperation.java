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

package com.hazelcast.executor.impl;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TraceableOperation;

import java.io.IOException;
import java.util.concurrent.Callable;

abstract class BaseCallableTaskOperation extends Operation implements TraceableOperation {

    protected String name;
    protected String uuid;
    protected transient Callable callable;
    private Data callableData;

    public BaseCallableTaskOperation() {
    }

    public BaseCallableTaskOperation(String name, String uuid, Data callableData) {
        this.name = name;
        this.uuid = uuid;
        this.callableData = callableData;
    }

    @Override
    public final void beforeRun() throws Exception {
        callable = getNodeEngine().toObject(callableData);
        ManagedContext managedContext = getManagedContext();

        if (callable instanceof RunnableAdapter) {
            RunnableAdapter adapter = (RunnableAdapter) callable;
            Runnable runnable = (Runnable) managedContext.initialize(adapter.getRunnable());
            adapter.setRunnable(runnable);
        } else {
            callable = (Callable) managedContext.initialize(callable);
        }
    }

    private ManagedContext getManagedContext() {
        HazelcastInstanceImpl hazelcastInstance = (HazelcastInstanceImpl) getNodeEngine().getHazelcastInstance();
        SerializationServiceImpl serializationService =
                (SerializationServiceImpl) hazelcastInstance.getSerializationService();
        return serializationService.getManagedContext();
    }

    @Override
    public final void run() throws Exception {
        DistributedExecutorService service = getService();
        service.execute(name, uuid, callable, getResponseHandler());
    }

    @Override
    public final void afterRun() throws Exception {
    }

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final Object getResponse() {
        return null;
    }

    @Override
    public Object getTraceIdentifier() {
        return uuid;
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
}
