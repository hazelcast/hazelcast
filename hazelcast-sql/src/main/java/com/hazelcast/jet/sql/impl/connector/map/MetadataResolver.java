/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.sql.impl.schema.IMapResolver;
import com.hazelcast.sql.impl.schema.Mapping;

import javax.annotation.Nullable;
import java.io.IOException;
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

        boolean hd = container.getMapConfig().getInMemoryFormat() == InMemoryFormat.NATIVE
                && !container.getMapConfig().getTieredStoreConfig().isEnabled();

        Metadata metadata = hd
                ? resolveFromContentsHd(iMapName, context)
                : resolveFromContents(iMapName, context);
        return metadata == null
                ? null
                : new Mapping(iMapName, iMapName, null, TYPE_NAME, null, metadata.fields(), metadata.options());
    }

    @Nullable
    private Metadata resolveFromContents(String name, MapServiceContext context) {
        for (PartitionContainer partitionContainer : context.getPartitionContainers()) {
            RecordStore<?> recordStore = partitionContainer.getExistingRecordStore(name);
            Metadata resolved = resolveFromContentsRecordStore(recordStore);
            if (resolved != null) {
                return resolved;
            }
        }
        return null;
    }

    private Metadata resolveFromContentsHd(String name, MapServiceContext context) {
        // Iterate only over local partitions.
        // MC will invoke this operation on each member until it finds some data.
        for (PartitionContainer partitionContainer : context.getPartitionContainers()) {
            // HD access must be from partition threads.
            GetAnyMetadataOperation op = new GetAnyMetadataOperation(name);
            Metadata resolved = invoke(op, partitionContainer.getPartitionId());
            if (resolved != null) {
                return resolved;
            }
        }
        return null;
    }

    private class GetAnyMetadataOperation extends Operation {
        private final String mapName;

        private GetAnyMetadataOperation(String mapName) {
            this.mapName = mapName;
        }

        @Override
        public void run() throws Exception {
            MapService service = getNodeEngine().getService(MapService.SERVICE_NAME);
            MapServiceContext context = service.getMapServiceContext();
            RecordStore<?> recordStore = context.getExistingRecordStore(getPartitionId(), mapName);
            // can return null, but that is fine
            sendResponse(resolveFromContentsRecordStore(recordStore));
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            throw new UnsupportedOperationException("This operation is invoked only locally");
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            throw new UnsupportedOperationException("This operation is invoked only locally");
        }
    }

    private <T extends Metadata> T invoke(Operation operation, int partitionId) {
        final InvocationFuture<T> future =
                nodeEngine.getOperationService().invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
        return future.joinInternal();
    }


    @Nullable
    @SuppressWarnings("rawtypes")
    private Metadata resolveFromContentsRecordStore(RecordStore<?> recordStore) {
        if (recordStore == null) {
            return null;
        }

        // some storage engines (Tiered Storage) require beforeOperation invocation
        // before using the record store.
        recordStore.beforeOperation();
        try {
            Iterator<Entry<Data, Record>> recordStoreIterator = recordStore.iterator();
            if (!recordStoreIterator.hasNext()) {
                return null;
            }

            Entry<Data, Record> entry = recordStoreIterator.next();
            return resolveMetadata(entry.getKey(), entry.getValue().getValue());
        } finally {
            recordStore.afterOperation();
        }
    }

    @Nullable
    private Metadata resolveMetadata(Object key, Object value) {
        InternalSerializationService ss = Util.getSerializationService(nodeEngine.getHazelcastInstance());

        Metadata keyMetadata = SampleMetadataResolver.resolve(ss, key, true);
        Metadata valueMetadata = SampleMetadataResolver.resolve(ss, value, false);
        return (keyMetadata != null && valueMetadata != null) ? keyMetadata.merge(valueMetadata) : null;
    }
}
