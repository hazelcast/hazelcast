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

package com.hazelcast.client;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;

import java.io.IOException;
import java.security.Permission;

/**
 * @ali 10/7/13
 */
public class ClientDestroyRequest extends CallableClientRequest implements Portable, RetryableRequest, SecureRequest{

    private String name;

    private String serviceName;

    public ClientDestroyRequest() {
    }

    public ClientDestroyRequest(String name, String serviceName) {
        this.name = name;
        this.serviceName = serviceName;
    }

    public Object call() throws Exception {
        getClientEngine().getProxyService().destroyDistributedObject(getServiceName(), name);
        return null;
    }

    public String getServiceName() {
        return serviceName;
    }

    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    public int getClassId() {
        return ClientPortableHook.DESTROY_PROXY;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n",name);
        writer.writeUTF("s", serviceName);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        serviceName = reader.readUTF("s");
    }

    public Permission getRequiredPermission() {
        return ActionConstants.getPermission(name, serviceName, ActionConstants.ACTION_DESTROY);
    }
}
