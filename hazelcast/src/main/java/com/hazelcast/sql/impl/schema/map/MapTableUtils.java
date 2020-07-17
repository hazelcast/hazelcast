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
import com.hazelcast.core.TypeConverter;
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
import com.hazelcast.query.impl.CompositeConverter;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.TypeConverters;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.sample.MapSampleMetadata;
import com.hazelcast.sql.impl.schema.map.sample.MapSampleMetadataResolver;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
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

    public static List<MapTableIndex> getPartitionedMapIndexes(MapContainer mapContainer, List<TableField> fields) {
        Map<QueryPath, Integer> pathToOrdinalMap = mapPathsToOrdinals(fields);

        List<Index> indexes = mapContainer.getIndexList();

        if (indexes.isEmpty()) {
            return Collections.emptyList();
        }

        List<MapTableIndex> res = new ArrayList<>(indexes.size());

        for (Index index : indexes) {
            IndexConfig indexConfig = index.getConfig();

            List<QueryDataType> resolvedFieldConverterTypes = indexConverterToSqlTypes(index.getConverter());

            List<Integer> indexFieldOrdinals = new ArrayList<>(indexConfig.getAttributes().size());
            List<QueryDataType> indexFieldConverterTypes = new ArrayList<>(indexConfig.getAttributes().size());

            for (int i = 0; i < indexConfig.getAttributes().size(); i++) {
                String attribute = indexConfig.getAttributes().get(i);

                QueryPath attributePath = QueryPath.create(attribute);

                Integer ordinal = pathToOrdinalMap.get(attributePath);

                if (ordinal == null) {
                    // No mapping for the field. Stop.
                    break;
                }

                if (i >= resolvedFieldConverterTypes.size()) {
                    // No more resolved converters. Stop.
                    break;
                }

                QueryDataType fieldType = fields.get(i).getType();
                QueryDataType converterType = resolvedFieldConverterTypes.get(i);

                if (!isCompatibleForIndexRequest(fieldType, converterType)) {
                    // Field and converter types are not compatible (e.g. INT vs VARCHAR).
                    break;
                }

                indexFieldOrdinals.add(ordinal);
                indexFieldConverterTypes.add(converterType);
            }

            MapTableIndex index0 = new MapTableIndex(
                indexConfig.getName(),
                indexConfig.getType(),
                indexFieldOrdinals,
                indexFieldConverterTypes
            );

            res.add(index0);
        }

        return res;
    }

    @SuppressWarnings("rawtypes")
    public static int getPartitionedMapDistributionField(
        MapContainer mapContainer,
        MapServiceContext context,
        List<TableField> fields
    ) {
        Map<QueryPath, Integer> pathToOrdinalMap = mapPathsToOrdinals(fields);

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

            if (fieldOrdinal != null && ((MapTableField) fields.get(fieldOrdinal)).isStaticallyTyped()) {
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
    private static Map<QueryPath, Integer> mapPathsToOrdinals(List<TableField> fields) {
        Map<QueryPath, Integer> res = new HashMap<>();

        for (int i = 0; i < fields.size(); i++) {
            res.put(((MapTableField) fields.get(i)).getPath(), i);
        }

        return res;
    }

    @SuppressWarnings("rawtypes")
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

    public static List<QueryDataType> indexConverterToSqlTypes(TypeConverter converter) {
        if (converter instanceof CompositeConverter) {
            CompositeConverter converter0 = ((CompositeConverter) converter);

            List<QueryDataType> res = new ArrayList<>(converter0.getComponentCount());

            for (int i = 0; i < converter0.getComponentCount(); i++) {
                QueryDataType type = indexConverterToSqlType(converter0.getComponentConverter(i));

                if (type == null) {
                    break;
                } else {
                    res.add(type);
                }
            }

            if (!res.isEmpty()) {
                return res;
            }
        } else {
            QueryDataType type = indexConverterToSqlType(converter);

            if (type != null) {
                return Collections.singletonList(type);
            }
        }

        return Collections.emptyList();
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static QueryDataType indexConverterToSqlType(TypeConverter converter) {
        assert !(converter instanceof CompositeConverter) : converter;

        if (converter == TypeConverters.BOOLEAN_CONVERTER) {
            return QueryDataType.BOOLEAN;
        } else if (converter == TypeConverters.BYTE_CONVERTER) {
            return QueryDataType.TINYINT;
        } else if (converter == TypeConverters.SHORT_CONVERTER) {
            return QueryDataType.SMALLINT;
        } else if (converter == TypeConverters.INTEGER_CONVERTER) {
            return QueryDataType.INT;
        } else if (converter == TypeConverters.LONG_CONVERTER) {
            return QueryDataType.BIGINT;
        } else if (converter == TypeConverters.BIG_DECIMAL_CONVERTER) {
            return QueryDataType.DECIMAL;
        } else if (converter == TypeConverters.BIG_INTEGER_CONVERTER) {
            return QueryDataType.DECIMAL_BIG_INTEGER;
        } else if (converter == TypeConverters.STRING_CONVERTER) {
            return QueryDataType.VARCHAR;
        } else if (converter == TypeConverters.CHAR_CONVERTER) {
            return QueryDataType.VARCHAR_CHARACTER;
        }

        // TODO: Add identity converter?

        return null;
    }

    private static boolean isCompatibleForIndexRequest(QueryDataType columnType, QueryDataType indexConverterType) {
        QueryDataTypeFamily indexConverterTypeFamily = indexConverterType.getTypeFamily();

        switch (columnType.getTypeFamily()) {
            case BOOLEAN:
                return indexConverterTypeFamily == QueryDataTypeFamily.BOOLEAN;

            case VARCHAR:
                return indexConverterTypeFamily == QueryDataTypeFamily.VARCHAR;

            default:
                return QueryDataTypeUtils.isNumeric(columnType) && QueryDataTypeUtils.isNumeric(indexConverterType);
        }
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
