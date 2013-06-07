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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.DrainOperation;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Collection;

/**
 * @ali 5/8/13
 */
public class DrainRequest extends QueueRequest {

    int maxSize;

    public DrainRequest() {
    }

    public DrainRequest(String name, int maxSize) {
        super(name);
        this.maxSize = maxSize;
    }

    protected Operation prepareOperation() {
        return new DrainOperation(name, maxSize);
    }

    public int getClassId() {
        return QueuePortableHook.DRAIN;
    }

    protected Object filter(Object response) {
        if (response instanceof SerializableCollection){
            Collection<Data> coll = ((SerializableCollection) response).getCollection();
            return new PortableCollection(coll);
        }
        return super.filter(response);
    }

    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        writer.writeInt("m",maxSize);
    }

    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        maxSize = reader.readInt("m");
    }
}
