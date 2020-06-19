/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.ExternalCatalog;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.sample.MapSampleMetadata;
import com.hazelcast.sql.impl.schema.map.sample.MapSampleMetadataResolver;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_REPLICATED;
import static java.util.Collections.emptyMap;

public class ReplicatedMapTableResolver extends AbstractMapTableResolver {

    private static final List<List<String>> SEARCH_PATHS =
        Collections.singletonList(Arrays.asList(QueryUtils.CATALOG, SCHEMA_NAME_REPLICATED));

    public ReplicatedMapTableResolver(NodeEngine nodeEngine) {
        super(nodeEngine, SEARCH_PATHS);
    }

    @Override @Nonnull
    public Collection<Table> getTables() {
        ReplicatedMapService mapService = nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);

        List<Table> res = new ArrayList<>();

        for (String mapName : mapService.getPartitionContainer(0).getStores().keySet()) {
            Table table;

            // TODO: skip all system tables, i.e. `__jet` prefixed
            if (mapName.equalsIgnoreCase(ExternalCatalog.CATALOG_MAP_NAME)) {
                continue;
            }

            try {
                table = createTable(nodeEngine, SCHEMA_NAME_REPLICATED, mapName, emptyMap(), null);
            } catch (QueryException e) {
                table = new ReplicatedMapTable(mapName, e);
            }

            res.add(table);
        }

        // TODO: Add pre-configured, but not started maps.

        return res;
    }

    @Nonnull
    public static Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mapName,
            @Nonnull Map<String, String> options,
            @Nullable List<TableField> fields
    ) {
        try {
            long estimatedRowCount = 0;
            QueryTargetDescriptor keyDescriptor = null;
            QueryTargetDescriptor valueDescriptor = null;

            if (fields == null) {
                ReplicatedMapService mapService = nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
                Collection<ReplicatedRecordStore> stores = mapService.getAllReplicatedRecordStores(mapName);

                // Iterate over the stores trying to get a sample.
                for (ReplicatedRecordStore store : stores) {
                    @SuppressWarnings("rawtypes")
                    Iterator<ReplicatedRecord> iterator = store.recordIterator();

                    if (!iterator.hasNext()) {
                        continue;
                    }

                    ReplicatedRecord<?, ?> record = iterator.next();

                    Object key = record.getKey();
                    Object value = record.getValue();

                    InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

                    // TODO: Resolve the flag properly (config.getInMemoryFormat() == InMemoryFormat.BINARY).
                    boolean binary = false;

                    MapSampleMetadata keyMetadata = MapSampleMetadataResolver.resolve(ss, key, binary, true);
                    MapSampleMetadata valueMetadata = MapSampleMetadataResolver.resolve(ss, value, binary, false);
                    keyDescriptor = keyMetadata.getDescriptor();
                    valueDescriptor = valueMetadata.getDescriptor();
                    estimatedRowCount = stores.size() * nodeEngine.getPartitionService().getPartitionCount();

                    fields = mergeMapFields(keyMetadata.getFields(), valueMetadata.getFields());
                    break;
                }

                if (fields == null) {
                    throw QueryException.error("Failed to resolve fields, map is empty: " + mapName);
                }
            }

            if (keyDescriptor == null) {
                keyDescriptor = new GenericQueryTargetDescriptor();
            }
            if (valueDescriptor == null) {
                valueDescriptor = new GenericQueryTargetDescriptor();
            }

            return new ReplicatedMapTable(
                    schemaName,
                    mapName,
                    fields,
                    new ConstantTableStatistics(estimatedRowCount),
                    keyDescriptor,
                    valueDescriptor,
                    options
            );
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw QueryException.error("Failed to get metadata for ReplicatedMap " + mapName + ": " + e.getMessage(), e);
        }
    }
}
