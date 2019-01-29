/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Iterator;

public class AddIndexOperation extends MapOperation implements PartitionAwareOperation, MutatingOperation {

    private String attributeName;
    private boolean ordered;

    public AddIndexOperation() {
    }

    public AddIndexOperation(String name, String attributeName, boolean ordered) {
        super(name);
        this.attributeName = attributeName;
        this.ordered = ordered;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        Indexes indexes = mapContainer.getIndexes(getPartitionId());
        Index index = indexes.addOrGetIndex(attributeName, ordered);

        final long now = getNow();
        final Iterator<Record> iterator = recordStore.iterator(now, false);
        SerializationService serializationService = getNodeEngine().getSerializationService();
        while (iterator.hasNext()) {
            final Record record = iterator.next();
            Data key = record.getKey();
            Object value = Records.getValueOrCachedValue(record, serializationService);
            QueryableEntry queryEntry = mapContainer.newQueryEntry(key, value);
            index.saveEntryIndex(queryEntry, null);
        }
    }

    private long getNow() {
        return Clock.currentTimeMillis();
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(attributeName);
        out.writeBoolean(ordered);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        attributeName = in.readUTF();
        ordered = in.readBoolean();
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.ADD_INDEX;
    }
}
