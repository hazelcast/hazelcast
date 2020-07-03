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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.sample.MapSampleMetadata;
import com.hazelcast.sql.impl.schema.map.sample.MapSampleMetadataResolver;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PARTITIONED;

public class PartitionedMapTableResolver extends AbstractMapTableResolver {

    private static final List<List<String>> SEARCH_PATHS =
        Collections.singletonList(Arrays.asList(QueryUtils.CATALOG, SCHEMA_NAME_PARTITIONED));

    public PartitionedMapTableResolver(NodeEngine nodeEngine) {
        super(nodeEngine, SEARCH_PATHS);
    }

    @Override @Nonnull
    public Collection<Table> getTables() {
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = mapService.getMapServiceContext();

        List<Table> res = new ArrayList<>();
        Set<String> knownNames = new HashSet<>();

        // Get started maps.
        for (String mapName : context.getMapContainers().keySet()) {
            PartitionedMapTable table = createTable(nodeEngine, context, mapName);

            if (table == null) {
                continue;
            }

            res.add(table);
            knownNames.add(mapName);
        }

        // Get maps that are not started locally yet.
        for (Map.Entry<String, MapConfig> configEntry : nodeEngine.getConfig().getMapConfigs().entrySet()) {
            String configMapName = configEntry.getKey();

            // Skip templates.
            if (configMapName.contains("*")) {
                continue;
            }

            if (knownNames.add(configMapName)) {
                res.add(emptyMap(configMapName));
            }
        }

        return res;
    }

    @SuppressWarnings({"rawtypes", "checkstyle:MethodLength", "checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    public static PartitionedMapTable createTable(
        NodeEngine nodeEngine,
        MapServiceContext context,
        String name
    ) {
        try {
            MapContainer mapContainer = context.getMapContainer(name);

            // Handle concurrent map destroy.
            if (mapContainer == null) {
                return null;
            }

            MapConfig config = mapContainer.getMapConfig();

            // HD maps are not supported at the moment.
            if (config.getInMemoryFormat() == InMemoryFormat.NATIVE) {
                throw QueryException.error("IMap with InMemoryFormat.NATIVE is not supported: " + name);
            }

            for (PartitionContainer partitionContainer : context.getPartitionContainers()) {
                // Resolve sample.
                RecordStore<?> recordStore = partitionContainer.getExistingRecordStore(name);

                if (recordStore == null) {
                    continue;
                }

                Iterator<Map.Entry<Data, Record>> recordStoreIterator = recordStore.iterator();

                if (!recordStoreIterator.hasNext()) {
                    continue;
                }

                Map.Entry<Data, Record> entry = recordStoreIterator.next();

                InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

                MapSampleMetadata keyMetadata = MapSampleMetadataResolver.resolve(
                    ss,
                    entry.getKey(),
                    true
                );

                MapSampleMetadata valueMetadata = MapSampleMetadataResolver.resolve(
                    ss,
                    entry.getValue().getValue(),
                    false
                );

                List<TableField> fields = mergeMapFields(keyMetadata.getFields(), valueMetadata.getFields());

                long estimatedRowCount = MapTableUtils.estimatePartitionedMapRowCount(nodeEngine, context, name);

                // Done.
                return new PartitionedMapTable(
                    name,
                    fields,
                    new ConstantTableStatistics(estimatedRowCount),
                    keyMetadata.getDescriptor(),
                    valueMetadata.getDescriptor()
                );
            }

            return emptyMap(name);
        } catch (QueryException e) {
            return new PartitionedMapTable(name, e);
        } catch (Exception e) {
            QueryException e0 = QueryException.error("Failed to get metadata for IMap " + name + ": " + e.getMessage(), e);

            return new PartitionedMapTable(name, e0);
        }
    }

    private static PartitionedMapTable emptyMap(String mapName) {
        QueryException error = QueryException.error(
            "Cannot resolve IMap schema because it doesn't have entries on the local member: " + mapName
        );

        return new PartitionedMapTable(mapName, error);
    }
}
