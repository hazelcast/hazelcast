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

package com.hazelcast.collection.operations.client;

import com.hazelcast.client.RetryableRequest;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.concurrent.lock.client.AbstractIsLockedRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;

import java.io.IOException;

/**
 * @author ali 5/23/13
 */
public class MultiMapIsLockedRequest extends AbstractIsLockedRequest implements RetryableRequest {

    String name;

    public MultiMapIsLockedRequest() {
    }

    public MultiMapIsLockedRequest(Data key, String name) {
        super(key);
        this.name = name;
    }

    protected ObjectNamespace getNamespace() {
        return new DefaultObjectNamespace(CollectionService.SERVICE_NAME, name);
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
        return CollectionPortableHook.F_ID;
    }

    public int getClassId() {
        return CollectionPortableHook.IS_LOCKED;
    }
}
