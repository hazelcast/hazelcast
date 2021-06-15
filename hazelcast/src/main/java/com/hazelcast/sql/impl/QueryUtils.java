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

package com.hazelcast.sql.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Common SQL engine utility methods used by both "core" and "sql" modules.
 */
public final class QueryUtils {

    public static final String CATALOG = "hazelcast";
    public static final String SCHEMA_NAME_PARTITIONED = "partitioned";

    public static final String WORKER_TYPE_FRAGMENT = "query-fragment-thread";
    public static final String WORKER_TYPE_SYSTEM = "query-system-thread";
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

    public static HazelcastSqlException toPublicException(Throwable e, UUID localMemberId) {
        if (e instanceof HazelcastSqlException) {
            return (HazelcastSqlException) e;
        }

        if (e instanceof QueryException) {
            QueryException e0 = (QueryException) e;

            UUID originatingMemberId = e0.getOriginatingMemberId();

            if (originatingMemberId == null) {
                originatingMemberId = localMemberId;
            }

            return new HazelcastSqlException(originatingMemberId, e0.getCode(), e0.getMessage(), e);
        } else {
            return new HazelcastSqlException(localMemberId, SqlErrorCode.GENERIC, e.getMessage(), e);
        }
    }

    /**
     * Convert internal column type to a public type.
     *
     * @param columnType Internal type.
     * @return Public type.
     */
    public static SqlColumnMetadata getColumnMetadata(String columnName, QueryDataType columnType, boolean columnIsNullable) {
        return new SqlColumnMetadata(columnName, columnType.getTypeFamily().getPublicType(), columnIsNullable);
    }

    /**
     * Create map from member ID to owned partitions.
     *
     * @param nodeEngine node engine
     * @param localMemberVersion version of the local member. If any of partition owners have a different version, an exception
     *                           is thrown. The check is ignored if passed version is {@code null}
     * @param failOnUnassignedPartition whether the call should fail in case an unassigned partition is found; when set to
     *                                  {@code false} the missing partitions will not be included in the result
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
                    ).markInvalidate();
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

    public static Address findLightJobCoordinator(NodeEngineImpl nodeEngine) {
        return findLightJobCoordinator(nodeEngine.getClusterService().getMembers(), nodeEngine.getLocalMember()).getAddress();
    }

    /**
     * Finds a larger same-version group of data members from a collection
     * members; if {@code localMember} is from that group, return that.
     * Otherwise return a random member from the group.
     * <p>
     * Used for SqlExecute and SubmitJob(light=true) messages.
     *
     * @param members list of all members
     * @param localMember the local member, null for client instance
     */
    @Nonnull
    public static Member findLightJobCoordinator(@Nonnull Collection<Member> members, @Nullable Member localMember) {
        // The members should have at most 2 different version (ignoring the patch version).
        // Find a random member from the larger same-version group.
        Version[] versions = new Version[2];
        int[] counts = new int[2];
        int grossMajority = members.size() / 2;

        for (Member m : members) {
            if (m.isLiteMember()) {
                continue;
            }
            Version v = m.getVersion().asVersion();
            int index;
            if (versions[0] == null || versions[0].equals(v)) {
                index = 0;
            } else if (versions[1] == null || versions[1].equals(v)) {
                index = 1;
            } else {
                throw new RuntimeException("More than 2 distinct member versions found: " + versions[0] + ", " + versions[1] + ", " + v);
            }
            versions[index] = v;
            counts[index]++;
            // a shortcut - always taken if there are no lite members and the cluster size is odd
            if (counts[index] > grossMajority) {
                return m;
            }
        }

        // no data members
        if (counts[0] == 0) {
            throw QueryException.error("No data member found");
        }

        int largerGroupIndex = counts[0] > counts[1] ? 0
                : counts[1] > counts[0] ? 1
                : versions[0].compareTo(versions[1]) > 0 ? 0 : 1;

        // if the local member is data member and is from the larger group, use that
        if (localMember != null && !localMember.isLiteMember()
                && localMember.getVersion().asVersion().equals(versions[largerGroupIndex])) {
            return localMember;
        }

        // otherwise return a random member from the larger group
        int randomMemberIndex = ThreadLocalRandom.current().nextInt(counts[largerGroupIndex]);
        for (Member m : members) {
            if (!m.isLiteMember() && m.getVersion().asVersion().equals(versions[largerGroupIndex])) {
                randomMemberIndex--;
                if (randomMemberIndex < 0) {
                    return m;
                }
            }
        }

        throw new RuntimeException("should never get here");
    }
}
