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

package com.hazelcast.map.impl.client;

import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.concurrent.lock.client.AbstractLockRequest;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;
import java.io.IOException;
import java.security.Permission;

public class MapLockRequest extends AbstractLockRequest implements SecureRequest {

    private String name;

    public MapLockRequest() {
    }

    public MapLockRequest(String name, Data key, long threadId) {
        super(key, threadId);
        this.name = name;
    }

    public MapLockRequest(String name, Data key, long threadId, long ttl, long timeout) {
        super(key, threadId, ttl, timeout);
        this.name = name;
    }

    protected ObjectNamespace getNamespace() {
        return new DefaultObjectNamespace(MapService.SERVICE_NAME, name);
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.LOCK;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        super.write(writer);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        super.read(reader);
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_LOCK);
    }

    @Override
    public String getDistributedObjectType() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }
}
