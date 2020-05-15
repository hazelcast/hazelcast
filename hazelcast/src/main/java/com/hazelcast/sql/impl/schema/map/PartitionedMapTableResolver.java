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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PARTITIONED;

// TODO: Error for empty map
// TODO: Error for HD map
// TODO: Proper field unwinding
public class PartitionedMapTableResolver extends AbstractMapTableResolver {

    private static final List<List<String>> SEARCH_PATHS =
        Collections.singletonList(Arrays.asList(QueryUtils.CATALOG, SCHEMA_NAME_PARTITIONED));

    public PartitionedMapTableResolver(NodeEngine nodeEngine) {
        super(nodeEngine, SEARCH_PATHS);
    }

    @Override
    public Collection<Table> getTables() {
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = mapService.getMapServiceContext();

        List<Table> res = new ArrayList<>();

        for (String mapName : context.getMapContainers().keySet()) {
            PartitionedMapTable table;

            try {
                table = createTable(context, mapName);
            } catch (QueryException e) {
                table = new PartitionedMapTable(mapName, e);
            }

            if (table == null) {
                continue;
            }

            res.add(table);
        }

        return res;
    }

    @SuppressWarnings({"rawtypes", "checkstyle:MethodLength", "checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private PartitionedMapTable createTable(MapServiceContext context, String mapName) {
        try {
            MapContainer mapContainer = context.getMapContainer(mapName);

            if (mapContainer == null) {
                return null;
            }

            for (PartitionContainer partitionContainer : context.getPartitionContainers()) {
                // Resolve sample.
                RecordStore<?> recordStore = partitionContainer.getExistingRecordStore(mapName);

                if (recordStore == null) {
                    continue;
                }

                Iterator<Map.Entry<Data, Record>> recordStoreIterator = recordStore.iterator();

                if (!recordStoreIterator.hasNext()) {
                    continue;
                }

                Map.Entry<Data, Record> entry = recordStoreIterator.next();

                InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

                MapSampleMetadata keyMetadata = MapSampleMetadataResolver.resolve(ss, entry.getKey(), true);
                MapSampleMetadata valueMetadata = MapSampleMetadataResolver.resolve(ss, entry.getValue().getValue(), false);
                long estimatedRowCount = recordStore.size() * nodeEngine.getPartitionService().getPartitionCount();

                List<TableField> fields = mergeMapFields(keyMetadata.getFields(), valueMetadata.getFields());

                // Done.
                return new PartitionedMapTable(
                    mapName,
                    fields,
                    new ConstantTableStatistics(estimatedRowCount),
                    keyMetadata.getDescriptor(),
                    valueMetadata.getDescriptor()
                );
            }

            // TODO: Throw and error here instead so that the user knows that resolution failed due to empty map.
            return null;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw QueryException.error("Failed to get metadata for IMap " + mapName + ": " + e.getMessage(), e);
        }
    }
}
