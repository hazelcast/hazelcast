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

package com.hazelcast.concurrent.countdownlatch.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchPortableHook;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * @ali 5/28/13
 */
public class CountDownLatchDestroyRequest extends CallableClientRequest implements Portable, RetryableRequest {

    String name;

    public CountDownLatchDestroyRequest() {
    }

    public CountDownLatchDestroyRequest(String name) {
        this.name = name;
    }

    public Object call() throws Exception {
        return null;
    }

    public String getServiceName() {
        return CountDownLatchService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return CountDownLatchPortableHook.F_ID;
    }

    public int getClassId() {
        return CountDownLatchPortableHook.DESTROY;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }
}
