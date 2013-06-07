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

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * @ali 5/24/13
 */
public class CollectionDestroyRequest extends CallableClientRequest implements Portable, RetryableRequest {

    private CollectionProxyId proxyId;

    public CollectionDestroyRequest() {
    }

    public CollectionDestroyRequest(CollectionProxyId proxyId) {
        this.proxyId = proxyId;
    }

    public Object call() throws Exception {
        final CollectionService service = getService();
        service.destroyDistributedObject(proxyId);
        return null;
    }

    public String getServiceName() {
        return CollectionService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return CollectionPortableHook.F_ID;
    }

    public int getClassId() {
        return CollectionPortableHook.DESTROY;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        proxyId.writeData(writer.getRawDataOutput());
    }

    public void readPortable(PortableReader reader) throws IOException {
        proxyId = new CollectionProxyId();
        proxyId.readData(reader.getRawDataInput());
    }
}
