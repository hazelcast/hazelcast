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

import com.hazelcast.multimap.MultiMapPortableHook;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.concurrent.lock.client.AbstractLockRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;

import java.io.IOException;

/**
 * @author ali 5/23/13
 */
public class MultiMapLockRequest extends AbstractLockRequest {

    String name;

    public MultiMapLockRequest() {
    }

    public MultiMapLockRequest(Data key, int threadId, String name) {
        super(key, threadId);
        this.name = name;
    }

    public MultiMapLockRequest(Data key, int threadId, long ttl, long timeout, String name) {
        super(key, threadId, ttl, timeout);
        this.name = name;
    }

    protected ObjectNamespace getNamespace() {
        return new DefaultObjectNamespace(MultiMapService.SERVICE_NAME, name);
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        super.writePortable(writer);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        super.readPortable(reader);
    }

    public int getFactoryId() {
        return MultiMapPortableHook.F_ID;
    }

    public int getClassId() {
        return MultiMapPortableHook.LOCK;
    }
}
