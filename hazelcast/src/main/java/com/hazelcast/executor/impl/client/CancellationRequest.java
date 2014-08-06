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

package com.hazelcast.executor.impl.client;

import com.hazelcast.client.InvocationClientRequest;
import com.hazelcast.executor.impl.CancellationOperation;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.ExecutorPortableHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;

import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.ExecutionException;

public class CancellationRequest extends InvocationClientRequest {

    static final int CANCEL_TRY_COUNT = 50;
    static final int CANCEL_TRY_PAUSE_MILLIS = 250;

    private String uuid;
    private Address target;
    private int partitionId = -1;
    private boolean interrupt;

    public CancellationRequest() {
    }

    public CancellationRequest(String uuid, Address target, boolean interrupt) {
        this.uuid = uuid;
        this.target = target;
        this.interrupt = interrupt;
    }

    public CancellationRequest(String uuid, int partitionId, boolean interrupt) {
        this.uuid = uuid;
        this.partitionId = partitionId;
        this.interrupt = interrupt;
    }

    @Override
    protected void invoke() {
        CancellationOperation op = new CancellationOperation(uuid, interrupt);
        InvocationBuilder builder;
        if (target != null) {
            builder = createInvocationBuilder(getServiceName(), op, target);
        } else {
            builder = createInvocationBuilder(getServiceName(), op, partitionId);
        }
        builder.setTryCount(CANCEL_TRY_COUNT).setTryPauseMillis(CANCEL_TRY_PAUSE_MILLIS);
        InternalCompletableFuture future = builder.invoke();
        boolean result = false;
        try {
            result = (Boolean) future.get();
        } catch (InterruptedException e) {
            logException(e);
        } catch (ExecutionException e) {
            logException(e);
        }
        getEndpoint().sendResponse(result, getCallId());
    }

    private void logException(Exception e) {
        ILogger logger = getClientEngine().getLogger(CancellationRequest.class);
        logger.warning(e);
    }

    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ExecutorPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ExecutorPortableHook.CANCELLATION_REQUEST;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("u", uuid);
        writer.writeInt("p", partitionId);
        writer.writeBoolean("i", interrupt);
        ObjectDataOutput rawDataOutput = writer.getRawDataOutput();
        rawDataOutput.writeObject(target);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        uuid = reader.readUTF("u");
        partitionId = reader.readInt("p");
        interrupt = reader.readBoolean("i");
        ObjectDataInput rawDataInput = reader.getRawDataInput();
        target = rawDataInput.readObject();
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
