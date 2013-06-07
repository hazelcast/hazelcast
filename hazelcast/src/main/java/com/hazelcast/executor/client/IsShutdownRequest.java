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

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.executor.ExecutorDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * @ali 5/27/13
 */
public class IsShutdownRequest extends CallableClientRequest implements IdentifiedDataSerializable, RetryableRequest {

    String name;

    public IsShutdownRequest() {
    }

    public IsShutdownRequest(String name) {
        this.name = name;
    }

    public Object call() throws Exception {
        final DistributedExecutorService service = getService();
        return service.isShutdown(name);
    }

    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ExecutorDataSerializerHook.F_ID;
    }

    public int getId() {
        return ExecutorDataSerializerHook.IS_SHUTDOWN_REQUEST;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }
}
