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

package com.hazelcast.sql.impl;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlException;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Common SQL engine utility methods used by both "core" and "sql" modules.
 */
public final class QueryUtils {

    public static final String CATALOG = "hazelcast";
    public static final String SCHEMA_NAME_PARTITIONED = "partitioned";

    public static final String WORKER_TYPE_OPERATION = "query-operation-thread";
    public static final String WORKER_TYPE_FRAGMENT = "query-fragment-thread";
    public static final String WORKER_TYPE_STATE_CHECKER = "query-state-checker";

    private QueryUtils() {
        // No-op.
    }

    public static String workerName(String instanceName, String workerType) {
        return instanceName + "-" + workerType;
    }

    public static String workerName(String instanceName, String workerType, long index) {
        return instanceName + "-" + workerType + "-" + index;
    }

    public static SqlException toPublicException(Exception e, UUID localMemberId) {
        if (e instanceof SqlException) {
            return (SqlException) e;
        }

        if (e instanceof QueryException) {
            QueryException e0 = (QueryException) e;

            UUID originatingMemberId = e0.getOriginatingMemberId();

            if (originatingMemberId == null) {
                originatingMemberId = localMemberId;
            }

            return new SqlException(originatingMemberId, e0.getCode(), e0.getMessage(), e);
        } else {
            return new SqlException(localMemberId, SqlErrorCode.GENERIC, e.getMessage(), e);
        }
    }

    /**
     * Convert internal column type to a public type.
     *
     * @param columnType Internal type.
     * @return Public type.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public static SqlColumnMetadata getColumnMetadata(String columnName, QueryDataType columnType) {
        SqlColumnType type;

        switch (columnType.getTypeFamily()) {
            case VARCHAR:
                type = SqlColumnType.VARCHAR;

                break;

            case BOOLEAN:
                type = SqlColumnType.BOOLEAN;

                break;

            case TINYINT:
                type = SqlColumnType.TINYINT;

                break;

            case SMALLINT:
                type = SqlColumnType.SMALLINT;

                break;

            case INTEGER:
                type = SqlColumnType.INTEGER;

                break;

            case BIGINT:
                type = SqlColumnType.BIGINT;

                break;

            case DECIMAL:
                type = SqlColumnType.DECIMAL;

                break;

            case REAL:
                type = SqlColumnType.REAL;

                break;

            case DOUBLE:
                type = SqlColumnType.DOUBLE;

                break;

            case TIME:
                type = SqlColumnType.TIME;

                break;

            case DATE:
                type = SqlColumnType.DATE;

                break;

            case TIMESTAMP:
                type = SqlColumnType.TIMESTAMP;

                break;

            case TIMESTAMP_WITH_TIME_ZONE:
                type = SqlColumnType.TIMESTAMP_WITH_TIME_ZONE;

                break;

            case NULL:
                type = SqlColumnType.NULL;
                break;

            default:
                assert columnType == QueryDataType.OBJECT;

                type = SqlColumnType.OBJECT;
        }

        return new SqlColumnMetadata(columnName, type);
    }

    public static Map<UUID, PartitionIdSet> createPartitionMap(NodeEngine nodeEngine) {
        Collection<Partition> parts = nodeEngine.getHazelcastInstance().getPartitionService().getPartitions();

        int partCnt = parts.size();

        Map<UUID, PartitionIdSet> partMap = new LinkedHashMap<>();

        for (Partition part : parts) {
            UUID ownerId = part.getOwner().getUuid();
            partMap.computeIfAbsent(ownerId, (key) -> new PartitionIdSet(partCnt)).add(part.getPartitionId());
        }

        return partMap;
    }

    public static List<List<String>> prepareSearchPaths(
        List<List<String>> currentSearchPaths,
        List<TableResolver> tableResolvers
    ) {
        // Current search paths have the highest priority.
        List<List<String>> res = new ArrayList<>();

        if (currentSearchPaths != null) {
            res.addAll(currentSearchPaths);
        }

        // Then add paths from table resolvers.
        if (tableResolvers != null) {
            for (TableResolver tableResolver : tableResolvers) {
                List<List<String>> tableResolverSearchPaths = tableResolver.getDefaultSearchPaths();

                if (tableResolverSearchPaths != null) {
                    res.addAll(tableResolverSearchPaths);
                }
            }
        }

        // Add catalog scope.
        res.add(Collections.singletonList(QueryUtils.CATALOG));

        // Add top-level scope.
        res.add(Collections.emptyList());

        return res;
    }
}
