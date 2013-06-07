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

package com.hazelcast.queue.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.QueueContainer;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.queue.QueueService;

import java.io.IOException;

/**
 * @ali 5/23/13
 */
public class RemainingCapacityRequest extends CallableClientRequest implements Portable, RetryableRequest {

    protected String name;

    public RemainingCapacityRequest() {
    }

    public RemainingCapacityRequest(String name) {
        this.name = name;
    }

    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    public int getClassId() {
        return QueuePortableHook.REMAINING_CAPACITY;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

    public Object call() throws Exception {
        QueueService service = getService();
        QueueContainer container = service.getOrCreateContainer(name, false);
        return container.getConfig().getMaxSize() - container.size();
    }

    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }
}
