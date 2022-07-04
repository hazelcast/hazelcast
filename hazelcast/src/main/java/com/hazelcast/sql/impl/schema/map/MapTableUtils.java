/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.impl.CompositeConverter;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.TypeConverters;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.util.ArrayList;
import java.util.Collections;
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

        PartitionIdSet ownerPartitions = context.getOrInitCachedMemberPartitions();

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

        if (mapContainer.getIndexes() == null) {
            return Collections.emptyList();
        }

        InternalIndex[] indexes = mapContainer.getIndexes().getIndexes();

        if (indexes == null || indexes.length == 0) {
            return Collections.emptyList();
        }

        List<MapTableIndex> res = new ArrayList<>(indexes.length);

        for (Index index : indexes) {
            IndexConfig indexConfig = index.getConfig();

            List<QueryDataType> resolvedFieldConverterTypes = indexConverterToSqlTypes(index.getConverter());

            List<String> indexAttributes = indexConfig.getAttributes();
            List<Integer> indexFieldOrdinals = new ArrayList<>(indexAttributes.size());
            List<QueryDataType> indexFieldConverterTypes = new ArrayList<>(indexAttributes.size());
            String[] components = index.getComponents();

            for (int i = 0; i < indexAttributes.size(); i++) {
                String attribute = indexAttributes.get(i);
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

                QueryDataType fieldType = fields.get(ordinal).getType();
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
                    components.length,
                    indexFieldOrdinals,
                    indexFieldConverterTypes
            );

            res.add(index0);
        }

        return res;
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

    public static List<QueryDataType> indexConverterToSqlTypes(TypeConverter converter) {
        if (converter == null) {
            return Collections.emptyList();
        }

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

    @SuppressWarnings({"checkstyle:ReturnCount", "checkstyle:CyclomaticComplexity"})
    public static QueryDataType indexConverterToSqlType(TypeConverter converter) {
        if (converter == null) {
            return null;
        }

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
        } else if (converter == TypeConverters.FLOAT_CONVERTER) {
            return QueryDataType.REAL;
        } else if (converter == TypeConverters.DOUBLE_CONVERTER) {
            return QueryDataType.DOUBLE;
        } else if (converter == TypeConverters.STRING_CONVERTER) {
            return QueryDataType.VARCHAR;
        } else if (converter == TypeConverters.CHAR_CONVERTER) {
            return QueryDataType.VARCHAR_CHARACTER;
        } else if (converter == TypeConverters.LOCAL_TIME_CONVERTER) {
            return QueryDataType.TIME;
        } else if (converter == TypeConverters.LOCAL_DATE_CONVERTER) {
            return QueryDataType.DATE;
        } else if (converter == TypeConverters.LOCAL_DATE_TIME_CONVERTER) {
            return QueryDataType.TIMESTAMP;
        } else if (converter == TypeConverters.OFFSET_DATE_TIME_CONVERTER) {
            return QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
        } else if (converter == TypeConverters.ENUM_CONVERTER) {
            return QueryDataType.OBJECT;
        } else if (converter == TypeConverters.IDENTITY_CONVERTER) {
            return QueryDataType.OBJECT;
        } else if (converter == TypeConverters.PORTABLE_CONVERTER) {
            return QueryDataType.OBJECT;
        } else if (converter == TypeConverters.UUID_CONVERTER) {
            return QueryDataType.OBJECT;
        }

        return null;
    }

    public static boolean isCompatibleForIndexRequest(QueryDataType columnType, QueryDataType indexConverterType) {
        if (columnType.getTypeFamily().equals(indexConverterType.getTypeFamily())) {
            return true;
        }

        return QueryDataTypeUtils.isNumeric(columnType) && QueryDataTypeUtils.isNumeric(indexConverterType);
    }
}
