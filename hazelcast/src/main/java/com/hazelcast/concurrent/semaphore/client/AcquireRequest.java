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

package com.hazelcast.concurrent.semaphore.client;

import com.hazelcast.concurrent.semaphore.AcquireOperation;
import com.hazelcast.concurrent.semaphore.SemaphorePortableHook;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 5/13/13
 */
public class AcquireRequest extends SemaphoreRequest {

    long timeout;

    public AcquireRequest() {
    }

    public AcquireRequest(String name, int permitCount, long timeout) {
        super(name, permitCount);
        this.timeout = timeout;
    }

    protected Operation prepareOperation() {
        return new AcquireOperation(name, permitCount, timeout);
    }

    public int getClassId() {
        return SemaphorePortableHook.ACQUIRE;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        writer.writeLong("t",timeout);
    }

    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        timeout = reader.readLong("t");
    }
}
