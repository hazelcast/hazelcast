/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

public class MapChunk extends Operation implements IdentifiedDataSerializable {

    protected static final AtomicInteger COUNT = new AtomicInteger();

    private transient String mapName;
    private transient MapChunkContext context;
    private transient LinkedList keyRecordExpiry;
    private transient BooleanSupplier isEndOfChunk;

    public MapChunk() {
    }

    public MapChunk(MapChunkContext context, BooleanSupplier isEndOfChunk) {
        this.context = context;
        this.isEndOfChunk = isEndOfChunk;
        System.err.println("Chunk number ----> " + COUNT.incrementAndGet()
                + ", mapName: " + context.getMapName() + ", partitionId: " + context.getPartitionId());
    }

    @Override
    public void run() throws Exception {
        assert !keyRecordExpiry.isEmpty() : "no empty operation expected";

        RecordStore recordStore = getRecordStore(mapName);

        do {
            Data dataKey = (Data) keyRecordExpiry.poll();
            Record record = (Record) keyRecordExpiry.poll();
            ExpiryMetadata expiryMetadata = (ExpiryMetadata) keyRecordExpiry.poll();

            // TODO add indexesMustBePopulated check into IndexingObserver
            recordStore.putOrUpdateReplicatedRecord(dataKey, record, expiryMetadata,
                    getReplicaIndex() == 0, Clock.currentTimeMillis());

        } while (!keyRecordExpiry.isEmpty());
    }

    private RecordStore getRecordStore(String mapName) {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getRecordStore(getPartitionId(), mapName, true);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        writeChunk((BufferObjectDataOutput) out, context);
    }

    private void writeChunk(BufferObjectDataOutput out, MapChunkContext context) throws IOException {
        String mapName = context.getMapName();
        SerializationService ss = context.getSerializationService();

        out.writeString(mapName);
        Iterator<Map.Entry<Data, Record>> entries = context.getIterator();

        while (entries.hasNext()) {
            Map.Entry<Data, Record> entry = entries.next();

            Data dataKey = entry.getKey();
            Record record = entry.getValue();
            Data dataValue = ss.toData(record.getValue());

            IOUtil.writeData(out, dataKey);
            Records.writeRecord(out, record, dataValue);
            Records.writeExpiry(out, context.getExpiryMetadata(dataKey));

            if (isEndOfChunk.getAsBoolean()) {
                break;
            }
        }

        // indicates end of chunk
        IOUtil.writeData(out, null);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        readChunk(in);
    }

    private void readChunk(ObjectDataInput in) throws IOException {
        this.mapName = in.readString();
        LinkedList keyRecordExpiry = new LinkedList<>();
        do {
            Data dataKey = IOUtil.readData(in);
            // null indicates end of chunk
            if (dataKey == null) {
                break;
            }

            Record record = Records.readRecord(in);
            ExpiryMetadata expiryMetadata = Records.readExpiry(in);

            keyRecordExpiry.add(dataKey);
            keyRecordExpiry.add(record);
            keyRecordExpiry.add(expiryMetadata);

        } while (true);

        this.keyRecordExpiry = keyRecordExpiry;

        System.err.println("Read chunk: " + keyRecordExpiry.size() / 3);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MAP_CHUNK;
    }

}
