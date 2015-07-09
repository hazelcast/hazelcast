/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ringbuffer.impl.client;

import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.ringbuffer.impl.operations.ReadOneOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

public class ReadOneRequest extends RingbufferRequest {

    private long sequence;

    public ReadOneRequest() {
    }

    public ReadOneRequest(String name, long sequence) {
        super(name);
        this.sequence = sequence;
    }

    @Override
    protected Operation prepareOperation() {
        return new ReadOneOperation(name, sequence);
    }

    @Override
    public int getClassId() {
        return RingbufferPortableHook.READ_ONE;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeLong("s", sequence);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        sequence = reader.readLong("s");
    }
}
