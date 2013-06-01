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
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.concurrent.lock.client.AbstractIsLockedRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;

import java.io.IOException;

/**
 * @ali 5/23/13
 */
public class MultiMapIsLockedRequest extends AbstractIsLockedRequest implements RetryableRequest {

    CollectionProxyId proxyId;

    public MultiMapIsLockedRequest() {
    }

    public MultiMapIsLockedRequest(Data key, CollectionProxyId proxyId) {
        super(key);
        this.proxyId = proxyId;
    }

    protected ObjectNamespace getNamespace() {
        return new DefaultObjectNamespace(CollectionService.SERVICE_NAME, proxyId);
    }

    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        proxyId.writeData(writer.getRawDataOutput());
    }

    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        proxyId = new CollectionProxyId();
        proxyId.readData(reader.getRawDataInput());
    }


    public int getFactoryId() {
        return CollectionPortableHook.F_ID;
    }

    public int getClassId() {
        return CollectionPortableHook.IS_LOCKED;
    }
}
