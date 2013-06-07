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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.queue.RemoveOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 5/8/13
 */
public class RemoveRequest extends QueueRequest {

    Data data;

    public RemoveRequest() {
    }

    public RemoveRequest(String name, Data data) {
        super(name);
        this.data = data;
    }

    protected Operation prepareOperation() {
        return new RemoveOperation(name, data);
    }

    public int getClassId() {
        return QueuePortableHook.REMOVE;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        data.writeData(out);
    }

    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        data = new Data();
        data.readData(in);
    }
}
