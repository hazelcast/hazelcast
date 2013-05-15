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

import com.hazelcast.client.MultiTargetClientRequest;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.AddInterceptorOperationFactory;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class MapAddInterceptorRequest extends MultiTargetClientRequest implements Portable {

    private String name;
    private MapInterceptor mapInterceptor;

    public MapAddInterceptorRequest() {
    }

    public MapAddInterceptorRequest(String name, MapInterceptor mapInterceptor) {
        this.name = name;
        this.mapInterceptor = mapInterceptor;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.ADD_INTERCEPTOR;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        final MapService mapService = getService();
        String id = mapService.addInterceptor(name, mapInterceptor);
        return new AddInterceptorOperationFactory(id, name, mapInterceptor);
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
            if(!member.localMember())
            addresses.add(member.getAddress());
        }
        return addresses;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(out);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        final ObjectDataInput in = reader.getRawDataInput();
        mapInterceptor = in.readObject();
    }

}
