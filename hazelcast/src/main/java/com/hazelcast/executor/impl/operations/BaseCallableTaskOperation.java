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

package com.hazelcast.executor.impl.operations;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.RunnableAdapter;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TraceableOperation;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.concurrent.Callable;

abstract class BaseCallableTaskOperation extends Operation implements TraceableOperation {

    protected String name;
    protected String uuid;
    protected transient Callable callable;
    private Data callableData;

    // transient.
    // We are cheating a bit here. The idea is the following. A BaseCallableTaskOperation is always going to be send to a
    // partition, but the operation doesn't send a response directly and therefor when a WrongTargetException is thronw (e.g.
    // partition has moved) the operation is not retried. To prevent this from happening, we say that we return a response until
    // the before-run method is called. Then we know we are going to be offloaded to a different thread and we are not returning
    // a response immediately. So then we switch to 'returnsResponse = false'.
    private boolean returnsResponse = true;

    public BaseCallableTaskOperation() {
    }

    public BaseCallableTaskOperation(String name, String uuid, Data callableData) {
        this.name = name;
        this.uuid = uuid;
        this.callableData = callableData;
    }

    @Override
    public final void beforeRun() throws Exception {
        returnsResponse = false;

        callable = getCallable();
        ManagedContext managedContext = getManagedContext();

        if (callable instanceof RunnableAdapter) {
            RunnableAdapter adapter = (RunnableAdapter) callable;
            Runnable runnable = (Runnable) managedContext.initialize(adapter.getRunnable());
            adapter.setRunnable(runnable);
        } else {
            callable = (Callable) managedContext.initialize(callable);
        }
    }

    /**
     * since this operation handles responses in an async way, we need to handle serialization exceptions too
     * @return
     */
    private Callable getCallable() {
        try {
            return getNodeEngine().toObject(callableData);
        } catch (HazelcastSerializationException e) {
            getResponseHandler().sendResponse(e);
            throw ExceptionUtil.rethrow(e);
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
        return returnsResponse;
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
}
