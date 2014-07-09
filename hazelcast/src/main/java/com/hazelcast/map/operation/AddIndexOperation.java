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

package com.hazelcast.map.operation;

import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.Iterator;

public class AddIndexOperation extends AbstractNamedOperation implements PartitionAwareOperation {

    String attributeName;
    boolean ordered;

    public AddIndexOperation(String name, String attributeName, boolean ordered) {
        super(name);
        this.attributeName = attributeName;
        this.ordered = ordered;
    }

    public AddIndexOperation() {
    }

    @Override
    public void run() throws Exception {
        MapService mapService = getService();
        MapContainer mapContainer = mapService.getMapServiceContext().getMapContainer(name);
        RecordStore recordStore = mapService.getMapServiceContext()
                .getPartitionContainer(getPartitionId()).getRecordStore(name);
        IndexService indexService = mapContainer.getIndexService();
        SerializationService ss = getNodeEngine().getSerializationService();
        Index index = indexService.addOrGetIndex(attributeName, ordered);
        final Iterator<Record> iterator = recordStore.valueIterator();
        while (iterator.hasNext()) {
            final Record record = iterator.next();
            Data key = record.getKey();
            Object value = record.getValue();
            index.saveEntryIndex(new QueryEntry(ss, key, key, value));
        }
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
}
