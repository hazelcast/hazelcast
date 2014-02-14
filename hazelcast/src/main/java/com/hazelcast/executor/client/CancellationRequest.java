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

package com.hazelcast.executor.client;

import com.hazelcast.client.InvocationClientRequest;
import com.hazelcast.executor.CancellationOperation;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.executor.ExecutorPortableHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author ali 11/02/14
 */
public class CancellationRequest extends InvocationClientRequest {

    static final int CANCEL_TRY_COUNT = 50;
    static final int CANCEL_TRY_PAUSE_MILLIS = 250;

    private String uuid;
    private Address target;
    private boolean interrupt;


    public CancellationRequest() {
    }

    public CancellationRequest(String uuid, Address target, boolean interrupt) {
        this.uuid = uuid;
        this.target = target;
        this.interrupt = interrupt;
    }

    protected void invoke() {
        final CancellationOperation op = new CancellationOperation(uuid, interrupt);
        final InvocationBuilder builder = createInvocationBuilder(getServiceName(), op, target);
        builder.setTryCount(CANCEL_TRY_COUNT).setTryPauseMillis(CANCEL_TRY_PAUSE_MILLIS);
        final InternalCompletableFuture future = builder.invoke();
        boolean result = false;
        try {
            result = (Boolean) future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        getEndpoint().sendResponse(result, getCallId());
    }

    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ExecutorPortableHook.F_ID;
    }

    public int getClassId() {
        return ExecutorPortableHook.CANCELLATION_REQUEST;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("u", uuid);
        writer.writeBoolean("i", interrupt);
        ObjectDataOutput rawDataOutput = writer.getRawDataOutput();
        target.writeData(rawDataOutput);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        uuid = reader.readUTF("u");
        interrupt = reader.readBoolean("i");
        ObjectDataInput rawDataInput = reader.getRawDataInput();
        target = new Address();
        target.readData(rawDataInput);
    }
}
