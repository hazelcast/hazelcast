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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.spi.properties.ClusterProperty.GLOBAL_HD_INDEX_ENABLED;
import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PARTITIONED;

public class PartitionedMapTableResolver extends AbstractMapTableResolver {

    private static final List<List<String>> SEARCH_PATHS =
            Collections.singletonList(Arrays.asList(QueryUtils.CATALOG, SCHEMA_NAME_PARTITIONED));

    public PartitionedMapTableResolver(NodeEngine nodeEngine, JetMapMetadataResolver jetMapMetadataResolver) {
        super(nodeEngine, jetMapMetadataResolver, SEARCH_PATHS);
    }

    @Override
    @Nonnull
    public List<Table> getTables() {
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
                res.add(emptyError(configMapName));
            }
        }

        return res;
    }

    private PartitionedMapTable createTable(
            NodeEngine nodeEngine,
            MapServiceContext context,
            String name
    ) {
        try {
            MapContainer mapContainer = context.getExistingMapContainer(name);

            // Handle concurrent map destroy.
            if (mapContainer == null) {
                return null;
            }

            boolean hd = nodeEngine.getProperties().getBoolean(GLOBAL_HD_INDEX_ENABLED);

            FieldsMetadata fieldsMetadata = null;

            if (hd) {
                fieldsMetadata = getHdMapFields(mapContainer);
            }

            if (fieldsMetadata == null || fieldsMetadata.emptyError) {
                fieldsMetadata = getHeapMapFields(context, name);
            }

            if (fieldsMetadata.emptyError) {
                return emptyError(name);
            }

            MapSampleMetadata keyMetadata = fieldsMetadata.keyMetadata;
            MapSampleMetadata valueMetadata = fieldsMetadata.valueMetadata;

            List<TableField> fields = mergeMapFields(keyMetadata.getFields(), valueMetadata.getFields());

            long estimatedRowCount = MapTableUtils.estimatePartitionedMapRowCount(nodeEngine, context, name);

            // Resolve indexes.
            List<MapTableIndex> indexes = MapTableUtils.getPartitionedMapIndexes(mapContainer, fields);

            // Done.
            return new PartitionedMapTable(
                SCHEMA_NAME_PARTITIONED,
                name,
                name,
                fields,
                new ConstantTableStatistics(estimatedRowCount),
                keyMetadata.getDescriptor(),
                valueMetadata.getDescriptor(),
                keyMetadata.getJetMetadata(),
                valueMetadata.getJetMetadata(),
                indexes,
                hd
            );
        } catch (QueryException e) {
            return new PartitionedMapTable(name, e);
        } catch (Exception e) {
            QueryException e0 = QueryException.error("Failed to get metadata for IMap " + name + ": " + e.getMessage(), e);

            return new PartitionedMapTable(name, e0);
        }
    }

    @SuppressWarnings("rawtypes")
    private FieldsMetadata getHeapMapFields(MapServiceContext context, String name) {
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

            return getFieldMetadata(entry.getKey(), entry.getValue().getValue());
        }

        return FieldsMetadata.EMPTY_ERROR;
    }

    @SuppressWarnings("rawtypes")
    private FieldsMetadata getHdMapFields(MapContainer mapContainer) {
        InternalIndex[] indexes = mapContainer.getIndexes().getIndexes();

        if (indexes == null || indexes.length == 0) {
            return FieldsMetadata.EMPTY_ERROR;
        }

        InternalIndex index = indexes[0];

        Iterator<QueryableEntry> entryIterator = index.getSqlRecordIterator(false);

        if (!entryIterator.hasNext()) {
            return FieldsMetadata.EMPTY_ERROR;
        }

        QueryableEntry entry = entryIterator.next();

        return getFieldMetadata(entry.getKey(), entry.getValue());
    }

    private static PartitionedMapTable emptyError(String mapName) {
        QueryException error = QueryException.error(
                "Couldn't resolve IMap schema automatically because " + mapName + " doesn't contain enough entries. "
                        + " Create an explicit mapping using 'CREATE MAPPING'"
        );

        return new PartitionedMapTable(mapName, error);
    }

    private FieldsMetadata getFieldMetadata(Object key, Object value) {
        InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

        MapSampleMetadata keyMetadata = MapSampleMetadataResolver.resolve(
                ss,
                jetMapMetadataResolver,
                key,
                true
        );

        MapSampleMetadata valueMetadata = MapSampleMetadataResolver.resolve(
                ss,
                jetMapMetadataResolver,
                value,
                false
        );

        return new FieldsMetadata(keyMetadata, valueMetadata);
    }

    private static final class FieldsMetadata {

        private static final FieldsMetadata EMPTY_ERROR = new FieldsMetadata(null, null, true);

        private final MapSampleMetadata keyMetadata;
        private final MapSampleMetadata valueMetadata;
        private final boolean emptyError;

        private FieldsMetadata(MapSampleMetadata keyMetadata, MapSampleMetadata valueMetadata) {
            this(keyMetadata, valueMetadata, false);
        }

        private FieldsMetadata(
                MapSampleMetadata keyMetadata,
                MapSampleMetadata valueMetadata,
                boolean emptyError
        ) {
            this.keyMetadata = keyMetadata;
            this.valueMetadata = valueMetadata;
            this.emptyError = emptyError;
        }
    }

    @Override
    public void registerListener(TableListener listener) {
    }
}
