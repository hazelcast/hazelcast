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

package com.hazelcast.spi.impl.proxyservice.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.spi.impl.SpiDataSerializerHook;

import java.io.IOException;

public class InitializeDistributedObjectOperation extends Operation implements IdentifiedDataSerializable {

    private String serviceName;
    private String name;

    public InitializeDistributedObjectOperation() {
    }

    public InitializeDistributedObjectOperation(String serviceName, String name) {
        this.serviceName = serviceName;
        this.name = name;
    }

    @Override
    public void run() throws Exception {
        ProxyService proxyService = getNodeEngine().getProxyService();
        proxyService.initializeDistributedObject(serviceName, name, getCallerUuid());
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(serviceName);
        out.writeObject(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        serviceName = in.readString();
        name = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.DIST_OBJECT_INIT;
    }
}
