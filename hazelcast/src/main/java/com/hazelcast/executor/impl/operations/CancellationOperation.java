/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.ExecutorDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

public final class CancellationOperation extends Operation implements NamedOperation, MutatingOperation,
        IdentifiedDataSerializable {

    private String uuid;
    private boolean interrupt;
    private boolean response;

    public CancellationOperation() {
    }

    public CancellationOperation(String uuid, boolean interrupt) {
        this.uuid = uuid;
        this.interrupt = interrupt;
    }

    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        DistributedExecutorService service = getService();
        response = service.cancel(uuid, interrupt);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public String getName() {
        DistributedExecutorService service = getService();
        return service.getName(uuid);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(uuid);
        out.writeBoolean(interrupt);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        uuid = in.readUTF();
        interrupt = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return ExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ExecutorDataSerializerHook.CANCELLATION;
    }

}
