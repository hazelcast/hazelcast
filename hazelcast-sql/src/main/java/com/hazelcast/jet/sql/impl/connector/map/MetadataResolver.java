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
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.schema.IMapResolver;
import com.hazelcast.sql.impl.schema.Mapping;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

        // HD maps must be accessed from correct thread, regular and Tiered Store
        // maps can be accessed from any thread.
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
            Metadata resolved = resolveFromContentsRecordStore(nodeEngine, recordStore);
            if (resolved != null) {
                return resolved;
            }
        }
        return null;
    }

    private Metadata resolveFromContentsHd(String name, MapServiceContext context) {
        // Iterate only over local partitions (owned and backups)
        // MC will invoke this operation on each member until it finds some data.

        for (PartitionContainer partitionContainer : context.getPartitionContainers()) {
            // HD access must be from partition threads
            GetAnyMetadataRunnable partitionTask = new GetAnyMetadataRunnable(context, name,
                    partitionContainer.getPartitionId());
            if (partitionTask.hasRecordStore()) {
                // Enqueue task only for partitions which may have some data
                nodeEngine.getOperationService().execute(partitionTask);
                if (partitionTask.getResult() != null) {
                    return partitionTask.getResult();
                }
            }
        }
        return null;
    }

    private static final class GetAnyMetadataRunnable implements PartitionSpecificRunnable {
        public static final int TIMEOUT = 5;
        private final MapServiceContext context;
        private final int partitionId;
        private final RecordStore<?> recordStore;
        private final CompletableFuture<Metadata> result = new CompletableFuture<>();

        GetAnyMetadataRunnable(MapServiceContext context, String mapName, int partitionId) {
            this.context = context;
            this.partitionId = partitionId;
            recordStore = context.getExistingRecordStore(getPartitionId(), mapName);
        }

        public boolean hasRecordStore() {
            return recordStore != null;
        }

        @Override
        public void run() {
            assert !result.isDone() : "This runnable can be executed once";
            // can return null, but that is fine
            result.complete(resolveFromContentsRecordStore(context.getNodeEngine(), recordStore));
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        public Metadata getResult() {
            try {
                return result.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // null is allowed response
                return null;
            } catch (ExecutionException | TimeoutException e) {
                throw new HazelcastSqlException("Cannot get sample data from map", e);
            }
        }
    }

    @Nullable
    @SuppressWarnings("rawtypes")
    private static Metadata resolveFromContentsRecordStore(NodeEngine nodeEngine, RecordStore<?> recordStore) {
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
            return resolveMetadata(nodeEngine, entry.getKey(), entry.getValue().getValue());
        } finally {
            recordStore.afterOperation();
        }
    }

    @Nullable
    private static Metadata resolveMetadata(NodeEngine nodeEngine, Object key, Object value) {
        InternalSerializationService ss = Util.getSerializationService(nodeEngine.getHazelcastInstance());

        // we need access to serialized key and value data to resolve serialization format
        // (compact, portable, generic, etc).
        Metadata keyMetadata = SampleMetadataResolver.resolve(ss, key, true);
        Metadata valueMetadata = SampleMetadataResolver.resolve(ss, value, false);
        return (keyMetadata != null && valueMetadata != null) ? keyMetadata.merge(valueMetadata) : null;
    }
}
