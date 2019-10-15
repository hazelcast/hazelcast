/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.internal.serialization.SerializationService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.ringbuffer.impl.client.RingbufferPortableHook.F_ID;
import static com.hazelcast.ringbuffer.impl.client.RingbufferPortableHook.READ_RESULT_SET;
import static java.util.Collections.unmodifiableList;

// This class is not used in serialized form since at least 3.9, however is still
// maintained as Portable for compatibility
public class PortableReadResultSet<E> implements Portable, ReadResultSet<E> {
    private transient long nextSeq;
    private transient long[] seqs;
    private List<Data> items;
    private int readCount;
    private SerializationService serializationService;

    public PortableReadResultSet() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public PortableReadResultSet(int readCount, List<Data> items, long[] seqs, long nextSeq) {
        this.readCount = readCount;
        this.items = items;
        this.seqs = seqs;
        this.nextSeq = nextSeq;
    }

    public List<Data> getDataItems() {
        return items;
    }

    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public Iterator<E> iterator() {
        List<E> result = new ArrayList<E>(items.size());
        for (Data data : items) {
            result.add((E) serializationService.toObject(data));
        }
        return unmodifiableList(result).iterator();
    }

    @Override
    public int readCount() {
        return readCount;
    }

    @Override
    public E get(int index) {
        Data data = items.get(index);
        return serializationService.toObject(data);
    }

    @Override
    public long getSequence(int index) {
        return seqs[index];
    }

    @Override
    public int size() {
        return items.size();
    }

    @Override
    public long getNextSequenceToReadFrom() {
        return nextSeq;
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
        for (Object item : items) {
            rawDataOutput.writeData((Data) item);
        }
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        readCount = reader.readInt("readCount");

        // reading the items.
        int size = reader.readInt("count");
        this.items = new ArrayList<Data>(size);
        ObjectDataInput rawDataInput = reader.getRawDataInput();
        for (int k = 0; k < size; k++) {
            Data item = rawDataInput.readData();
            items.add(item);
        }
    }
}
