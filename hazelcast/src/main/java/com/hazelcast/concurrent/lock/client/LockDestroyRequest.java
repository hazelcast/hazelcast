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

package com.hazelcast.concurrent.lock.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.concurrent.lock.LockPortableHook;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * @author ali 5/28/13
 */
public class LockDestroyRequest extends CallableClientRequest implements Portable, RetryableRequest {

    Object objectId;

    public LockDestroyRequest() {
    }

    public LockDestroyRequest(Object objectId) {
        this.objectId = objectId;
    }

    public Object call() throws Exception {
        getClientEngine().getProxyService().destroyDistributedObject(getServiceName(), objectId);
        return null;
    }

    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return LockPortableHook.FACTORY_ID;
    }

    public int getClassId() {
        return LockPortableHook.DESTROY;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.getRawDataOutput().writeObject(objectId);
    }

    public void readPortable(PortableReader reader) throws IOException {
        objectId = reader.getRawDataInput().readObject();
    }
}
