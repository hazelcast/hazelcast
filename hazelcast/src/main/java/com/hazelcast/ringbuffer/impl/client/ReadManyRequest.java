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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.impl.operations.ReadManyOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

public class ReadManyRequest extends RingbufferRequest {

    private long startSequence;
    private int minCount;
    private int maxCount;

    public ReadManyRequest() {
    }

    public ReadManyRequest(String name, long startSequence, int minCount, int maxCount, Data filter) {
        this.name = name;
        this.startSequence = startSequence;
        this.minCount = minCount;
        this.maxCount = maxCount;
    }

    @Override
    protected Operation prepareOperation() {
        return new ReadManyOperation(name, startSequence, minCount, maxCount, null);
    }

    @Override
    public int getClassId() {
        return RingbufferPortableHook.READ_MANY;
    }

    // here we convert the normal ReadResultSet to a PortableReadResultSet
    @Override
    protected Object filter(Object response) {
        ReadResultSet readResultSet = (ReadResultSet) response;
        int readCount = readResultSet.readCount();
        List<Object> items = new ArrayList<Object>(readCount);
        for (Object item : readResultSet) {
            items.add(item);
        }
        return new PortableReadResultSet<Object>(readCount, items);
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeLong("s", startSequence);
        writer.writeInt("i", minCount);
        writer.writeInt("a", maxCount);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        this.startSequence = reader.readLong("s");
        this.minCount = reader.readInt("i");
        this.maxCount = reader.readInt("a");
    }
}
