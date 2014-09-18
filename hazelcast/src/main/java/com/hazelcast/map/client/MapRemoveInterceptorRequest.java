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

package com.hazelcast.map.client;

import com.hazelcast.client.impl.client.MultiTargetClientRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.operation.RemoveInterceptorOperationFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;
import java.security.Permission;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class MapRemoveInterceptorRequest extends MultiTargetClientRequest implements Portable, SecureRequest {

    private String name;
    private String id;

    public MapRemoveInterceptorRequest() {
    }

    public MapRemoveInterceptorRequest(String name, String id) {
        this.name = name;
        this.id = id;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.REMOVE_INTERCEPTOR;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new RemoveInterceptorOperationFactory(id, name);
    }

    @Override
    protected Object reduce(Map<Address, Object> map) {
        return true;
    }

    @Override
    public Collection<Address> getTargets() {
        Collection<MemberImpl> memberList = getClientEngine().getClusterService().getMemberList();
        Collection<Address> addresses = new HashSet<Address>();
        for (MemberImpl member : memberList) {
            addresses.add(member.getAddress());
        }
        return addresses;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("id", id);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        id = reader.readUTF("id");
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_INTERCEPT);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "removeInterceptor";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{id};
    }
}
