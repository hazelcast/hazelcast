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
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.DeclarativePartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}
