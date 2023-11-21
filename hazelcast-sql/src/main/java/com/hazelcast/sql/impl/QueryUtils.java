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

package com.hazelcast.sql.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastObjectType;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.version.MemberVersion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static java.util.stream.Collectors.toList;

/**
 * Common SQL engine utility methods used by both "core" and "sql" modules.
 */
public final class QueryUtils {

    public static final String SCHEMA_NAME_PUBLIC = "public";
    public static final String SCHEMA_NAME_INFORMATION_SCHEMA = "information_schema";
    public static final String CATALOG = "hazelcast";

    public static final String WORKER_TYPE_STATE_CHECKER = "query-state-checker";

    // This is an arbitrarily-chosen prefix so that data connection names don't clash with other object names
    private static final String DATA_CONNECTION_KEY_PREFIX = "57ae1d3a-d379-44cb-bb60-86b1d2dcd744-";

    // TODO: tech debt: revisit and this class, move some methods to other classes.

    private QueryUtils() {
        // No-op.
    }

    public static String workerName(String instanceName, String workerType) {
        return instanceName + "-" + workerType;
    }

    public static String wrapDataConnectionKey(String dataConnectionKey) {
        return DATA_CONNECTION_KEY_PREFIX + dataConnectionKey;
    }

    /**
     * Convert internal column type to a public type.
     *
     * @param columnType Internal type.
     * @return Public type.
     */
    public static SqlColumnMetadata getColumnMetadata(String columnName, QueryDataType columnType,
                                                      boolean columnIsNullable) {
        return new SqlColumnMetadata(columnName, columnType.getTypeFamily().getPublicType(), columnIsNullable);
    }

    /**
     * Create map from member ID to owned partitions.
     *
     * @param nodeEngine                node engine
     * @param localMemberVersion        version of the local member. If any of partition owners have a different
     *                                  version, an exception is thrown.
     *                                  The check is ignored if passed version is {@code null}
     * @param failOnUnassignedPartition whether the call should fail in case an unassigned partition is found;
     *                                  when set to {@code false} the missing partitions will not be included
     *                                  in the result
     * @return partition mapping
     */
    public static Map<UUID, PartitionIdSet> createPartitionMap(
            NodeEngine nodeEngine,
            @Nullable MemberVersion localMemberVersion,
            boolean failOnUnassignedPartition
    ) {
        Collection<Partition> parts = nodeEngine.getHazelcastInstance().getPartitionService().getPartitions();

        int partCnt = parts.size();

        Map<UUID, PartitionIdSet> partMap = new LinkedHashMap<>();

        for (Partition part : parts) {
            Member owner = part.getOwner();

            if (owner == null) {
                if (failOnUnassignedPartition) {
                    throw QueryException.error(
                            SqlErrorCode.PARTITION_DISTRIBUTION,
                            "Partition is not assigned to any member: " + part.getPartitionId()
                    );
                } else {
                    continue;
                }
            }

            if (localMemberVersion != null) {
                if (!localMemberVersion.equals(owner.getVersion())) {
                    UUID localMemberId = nodeEngine.getLocalMember().getUuid();

                    throw QueryException.error("Cannot execute SQL query when members have different versions "
                            + "(make sure that all members have the same version) {localMemberId=" + localMemberId
                            + ", localMemberVersion=" + localMemberVersion + ", remoteMemberId=" + owner.getUuid()
                            + ", remoteMemberVersion=" + owner.getVersion() + "}");
                }
            }

            partMap.computeIfAbsent(owner.getUuid(), (key) -> new PartitionIdSet(partCnt)).add(part.getPartitionId());
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
                res.addAll(tableResolver.getDefaultSearchPaths());
            }
        }

        // Add catalog scope.
        res.add(Collections.singletonList(QueryUtils.CATALOG));

        // Add top-level scope.
        res.add(Collections.emptyList());

        return res;
    }

    /**
     * Check if the given type contains cycles.
     */
    public static boolean containsCycles(final HazelcastObjectType type, final Set<String> discovered) {
        if (!discovered.add(type.getTypeName())) {
            return true;
        }

        for (final RelDataTypeField field : type.getFieldList()) {
            final RelDataType fieldType = field.getType();
            if (fieldType instanceof HazelcastObjectType
                    && containsCycles((HazelcastObjectType) fieldType, discovered)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Quote the given compound identifier using Calcite dialect (= Hazelcast dialect).
     * You can use this when giving information to the user, e.g. in exception message.
     * When building a query you should use {@link SqlDialect#quoteIdentifier(StringBuilder, List)} directly.
     */
    public static String quoteCompoundIdentifier(String... compoundIdentifier) {
        List<String> parts = Arrays.stream(compoundIdentifier).filter(Objects::nonNull).collect(toList());
        return CalciteSqlDialect.DEFAULT
                .quoteIdentifier(new StringBuilder(), parts)
                .toString();
    }

    public static <K, V> MapContainer getMapContainer(IMap<K, V> map) {
        MapProxyImpl<K, V> mapProxy = (MapProxyImpl<K, V>) map;
        MapService mapService = mapProxy.getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapContainer(map.getName());
    }
}
