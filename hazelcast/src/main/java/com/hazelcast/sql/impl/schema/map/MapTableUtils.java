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

import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.DeclarativePartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.sample.MapSampleMetadata;
import com.hazelcast.sql.impl.schema.map.sample.MapSampleMetadataResolver;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Utility methods for schema resolution.
 */
public final class MapTableUtils {

    private MapTableUtils() {
        // No-op.
    }

    public static long estimatePartitionedMapRowCount(NodeEngine nodeEngine, MapServiceContext context, String mapName) {
        long entryCount = 0L;

        PartitionIdSet ownerPartitions = context.getOwnedPartitions();

        for (PartitionContainer partitionContainer : context.getPartitionContainers()) {
            if (!ownerPartitions.contains(partitionContainer.getPartitionId())) {
                continue;
            }

            RecordStore<?> recordStore = partitionContainer.getExistingRecordStore(mapName);

            if (recordStore == null) {
                continue;
            }

            entryCount += recordStore.size();
        }

        int memberCount = nodeEngine.getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR).size();

        return entryCount * memberCount;
    }

    public static List<MapTableIndex> getPartitionedMapIndexes(
        MapContainer mapContainer,
        String mapName,
        Map<QueryPath, Integer> pathToOrdinalMap
    ) {
        List<MapTableIndex> res = new ArrayList<>(mapContainer.getIndexDefinitions().size());

        for (IndexConfig indexConfig : mapContainer.getIndexDefinitions().values()) {
            List<Integer> indexFieldOrdinals = new ArrayList<>(indexConfig.getAttributes().size());

            for (String attribute : indexConfig.getAttributes()) {
                QueryPath attributePath = QueryPath.create(attribute);

                Integer ordinal = pathToOrdinalMap.get(attributePath);

                if (ordinal == null) {
                    // No mapping for the field. Stop.
                    break;
                }

                indexFieldOrdinals.add(ordinal);
            }

            if (indexFieldOrdinals.isEmpty()) {
                // Failed to resolve a prefix of the index, so it cannot be used in any query => skip.
                break;
            }

            res.add(new MapTableIndex(mapName, indexConfig.getType(), indexFieldOrdinals));
        }

        return res;
    }

    @SuppressWarnings("rawtypes")
    public static int getPartitionedMapDistributionField(
        MapContainer mapContainer,
        MapServiceContext context,
        Map<QueryPath, Integer> pathToOrdinalMap
    ) {
        int distributionFieldOrdinal = PartitionedMapTable.DISTRIBUTION_FIELD_ORDINAL_NONE;

        MapConfig mapConfig = mapContainer.getMapConfig();

        PartitioningStrategy partitioningStrategy = context.getPartitioningStrategy(
            mapConfig.getName(),
            mapConfig.getPartitioningStrategyConfig()
        );

        if (partitioningStrategy instanceof DeclarativePartitioningStrategy) {
            String field = ((DeclarativePartitioningStrategy) partitioningStrategy).getField();
            QueryPath fieldPath = new QueryPath(field, true);
            Integer fieldOrdinal = pathToOrdinalMap.get(fieldPath);

            if (fieldOrdinal != null) {
                distributionFieldOrdinal = fieldOrdinal;
            }
        }

        return distributionFieldOrdinal;
    }

    /**
     * Given the field list assign ordinals to fields.
     *
     * @param fields Fields.
     * @return Map from field path to ordinal.
     */
    public static Map<QueryPath, Integer> mapPathsToOrdinals(List<TableField> fields) {
        Map<QueryPath, Integer> res = new HashMap<>();

        for (int i = 0; i < fields.size(); i++) {
            res.put(((MapTableField) fields.get(i)).getPath(), i);
        }

        return res;
    }

    @Nullable
    public static ResolveResult resolvePartitionedMap(InternalSerializationService ss, MapServiceContext context, String name) {
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

            Iterator<Entry<Data, Record>> recordStoreIterator = recordStore.iterator();

            if (!recordStoreIterator.hasNext()) {
                continue;
            }

            Entry<Data, Record> entry = recordStoreIterator.next();

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

            return new ResolveResult(
                    mergeMapFields(keyMetadata.getFields(), valueMetadata.getFields()),
                    keyMetadata.getDescriptor(),
                    valueMetadata.getDescriptor());
        }

        // no sample entry found on local member
        return null;
    }

    private static List<TableField> mergeMapFields(Map<String, TableField> keyFields, Map<String, TableField> valueFields) {
        LinkedHashMap<String, TableField> res = new LinkedHashMap<>(keyFields);

        for (Entry<String, TableField> valueFieldEntry : valueFields.entrySet()) {
            // Value fields do not override key fields.
            res.putIfAbsent(valueFieldEntry.getKey(), valueFieldEntry.getValue());
        }

        return new ArrayList<>(res.values());
    }

    public static final class ResolveResult {
        private final List<TableField> fields;
        private final QueryTargetDescriptor keyDescriptor;
        private final QueryTargetDescriptor valueDescriptor;

        public ResolveResult(
                List<TableField> fields,
                QueryTargetDescriptor keyDescriptor,
                QueryTargetDescriptor valueDescriptor
        ) {
            this.fields = fields;
            this.keyDescriptor = keyDescriptor;
            this.valueDescriptor = valueDescriptor;
        }

        public List<TableField> getFields() {
            return fields;
        }

        public QueryTargetDescriptor getKeyDescriptor() {
            return keyDescriptor;
        }

        public QueryTargetDescriptor getValueDescriptor() {
            return valueDescriptor;
        }
    }
}
