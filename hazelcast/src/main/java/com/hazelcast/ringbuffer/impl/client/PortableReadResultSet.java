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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.ringbuffer.ReadResultSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.ringbuffer.impl.client.RingbufferPortableHook.F_ID;
import static com.hazelcast.ringbuffer.impl.client.RingbufferPortableHook.READ_RESULT_SET;

public class PortableReadResultSet<E> implements Portable, ReadResultSet<E> {

    private List<E> items;
    private int readCount;

    public PortableReadResultSet() {
    }

    public PortableReadResultSet(int readCount, List<E> items) {
        this.readCount = readCount;
        this.items = items;
    }

    @Override
    public Iterator<E> iterator() {
        return items.iterator();
    }

    @Override
    public int readCount() {
        return readCount;
    }

    @Override
    public E get(int index) {
        return items.get(index);
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getClassId() {
        return READ_RESULT_SET;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("readCount", readCount);

        // writing the items
        writer.writeInt("count", items.size());
        ObjectDataOutput rawDataOutput = writer.getRawDataOutput();
        for (E item : items) {
            rawDataOutput.writeObject(item);
        }
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        readCount = reader.readInt("readCount");

        // reading the items.
        int size = reader.readInt("count");
        List<E> items = new ArrayList<E>(size);
        ObjectDataInput rawDataInput = reader.getRawDataInput();
        for (int k = 0; k < size; k++) {
            E item = (E) rawDataInput.readObject();
            items.add(item);
        }
        this.items = Collections.unmodifiableList(items);
    }
}
