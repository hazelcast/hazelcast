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

package com.hazelcast.multimap.operations.client;

import com.hazelcast.concurrent.lock.client.AbstractUnlockRequest;
import com.hazelcast.multimap.MultiMapPortableHook;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;
import java.io.IOException;
import java.security.Permission;

public class MultiMapUnlockRequest extends AbstractUnlockRequest {

    String name;

    public MultiMapUnlockRequest() {
    }

    public MultiMapUnlockRequest(Data key, long threadId, String name) {
        super(key, threadId);
        this.name = name;
    }

    public MultiMapUnlockRequest(Data key, long threadId, boolean force, String name) {
        super(key, threadId, force);
        this.name = name;
    }

    protected ObjectNamespace getNamespace() {
        return new DefaultObjectNamespace(MultiMapService.SERVICE_NAME, name);
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        super.write(writer);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        super.read(reader);
    }

    public int getFactoryId() {
        return MultiMapPortableHook.F_ID;
    }

    public int getClassId() {
        return MultiMapPortableHook.UNLOCK;
    }

    public Permission getRequiredPermission() {
        return new MultiMapPermission(name, ActionConstants.ACTION_LOCK);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getDistributedObjectType() {
        return MultiMapService.SERVICE_NAME;
    }
}
