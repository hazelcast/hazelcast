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

package com.hazelcast.map.clientv2;

import com.hazelcast.clientv2.AbstractClientRequest;
import com.hazelcast.clientv2.ClientRequest;
import com.hazelcast.map.DeleteOperation;
import com.hazelcast.map.MapFlushOperation;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class MapFlushRequest extends AbstractClientRequest implements ClientRequest {

    protected String name;

    public MapFlushRequest() {
    }

    public MapFlushRequest(String name) {
        this.name = name;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.FLUSH;
    }

    public Object process() throws Exception {
        MapFlushOperation op = new MapFlushOperation(name);
//        return clientEngine.invoke(getServiceName(), op, key);
        // todo implement
        return null;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

}
