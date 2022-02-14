/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.IMapResolver;
import com.hazelcast.sql.impl.schema.Mapping;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map.Entry;

import static com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector.TYPE_NAME;

public class MetadataResolver implements IMapResolver {

    private final NodeEngine nodeEngine;

    public MetadataResolver(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Nullable
    @Override
    public Mapping resolve(String iMapName) {
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = service.getMapServiceContext();
        MapContainer container = context.getExistingMapContainer(iMapName);
        if (container == null) {
            return null;
        }

        boolean hd = container.getMapConfig().getInMemoryFormat() == InMemoryFormat.NATIVE;
        Metadata metadata = hd ? resolveFromHd(container) : resolveFromHeap(iMapName, context);
        return metadata == null ? null : new Mapping(iMapName, iMapName, TYPE_NAME, metadata.fields(), metadata.options());
    }

    @Nullable
    @SuppressWarnings("rawtypes")
    private Metadata resolveFromHd(MapContainer container) {
        if (container.getIndexes() == null) {
            return null;
        }

        InternalIndex[] indexes = container.getIndexes().getIndexes();
        if (indexes == null || indexes.length == 0) {
            return null;
        }

        InternalIndex index = indexes[0];
        Iterator<QueryableEntry> entryIterator = index.getSqlRecordIterator(false);
        if (!entryIterator.hasNext()) {
            return null;
        }

        QueryableEntry entry = entryIterator.next();
        return resolveMetadata(entry.getKey(), entry.getValue());
    }

    @Nullable
    @SuppressWarnings("rawtypes")
    private Metadata resolveFromHeap(String name, MapServiceContext context) {
        for (PartitionContainer partitionContainer : context.getPartitionContainers()) {
            RecordStore<?> recordStore = partitionContainer.getExistingRecordStore(name);
            if (recordStore == null) {
                continue;
            }

            Iterator<Entry<Data, Record>> recordStoreIterator = recordStore.iterator();
            if (!recordStoreIterator.hasNext()) {
                continue;
            }

            Entry<Data, Record> entry = recordStoreIterator.next();
            return resolveMetadata(entry.getKey(), entry.getValue().getValue());
        }
        return null;
    }

    @Nullable
    private Metadata resolveMetadata(Object key, Object value) {
        InternalSerializationService ss = Util.getSerializationService(nodeEngine.getHazelcastInstance());

        Metadata keyMetadata = SampleMetadataResolver.resolve(ss, key, true);
        Metadata valueMetadata = SampleMetadataResolver.resolve(ss, value, false);
        return (keyMetadata != null && valueMetadata != null) ? keyMetadata.merge(valueMetadata) : null;
    }
}
