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
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;

import java.io.IOException;

public class DistributedObjectDestroyOperation
        extends Operation implements IdentifiedDataSerializable {

    private String serviceName;
    private String name;

    public DistributedObjectDestroyOperation() {
    }

    public DistributedObjectDestroyOperation(String serviceName, String name) {
        this.serviceName = serviceName;
        this.name = name;
    }

    @Override
    public void run() throws Exception {
        ProxyServiceImpl proxyService = getNodeEngine().getService(ProxyServiceImpl.SERVICE_NAME);
        proxyService.destroyLocalDistributedObject(serviceName, name, getCallerUuid(), false);
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(serviceName);
        out.writeString(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        serviceName = in.readString();
        name = in.readString();
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.DIST_OBJECT_DESTROY;
    }
}
