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

package com.hazelcast.sql.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.NodeEngine;
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

    public static final String WORKER_TYPE_STATE_CHECKER = "query-state-checker";

    private QueryUtils() {
        // No-op.
    }

    public static String workerName(String instanceName, String workerType) {
        return instanceName + "-" + workerType;
    }

    public static HazelcastSqlException toPublicException(Throwable e, @Nonnull UUID localMemberId) {
        if (e instanceof HazelcastSqlException) {
            return (HazelcastSqlException) e;
        }

        if (e instanceof QueryException) {
            QueryException e0 = (QueryException) e;

            UUID originatingMemberId = e0.getOriginatingMemberId();

            if (originatingMemberId == null) {
                originatingMemberId = localMemberId;
            }

            return new HazelcastSqlException(originatingMemberId, e0.getCode(), e0.getMessage(), e, e0.getSuggestion());
        } else {
            return new HazelcastSqlException(localMemberId, SqlErrorCode.GENERIC, e.getMessage(), e, null);
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
     * Finds a larger same-version group of data members from a collection of
     * members and, if {@code localMember} is from that group, return that.
     * Otherwise return a random member from the group. If the same-version
     * groups have the same size, return a member from the newer group
     * (preferably the local one).
     * <p>
     * Used for SqlExecute and SubmitJob(light=true) messages.
     *
     * @param members list of all members
     * @param localMember the local member, null for client instance
     * @return the chosen member or null, if no data member is found
     */
    @Nullable
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    public static Member memberOfLargerSameVersionGroup(@Nonnull Collection<Member> members, @Nullable Member localMember) {
        // The members should have at most 2 different version (ignoring the patch version).
        // Find a random member from the larger same-version group.

        // we don't use 2-element array to save on GC litter
        Version version0 = null;
        Version version1 = null;
        int count0 = 0;
        int count1 = 0;
        int grossMajority = members.size() / 2;

        for (Member m : members) {
            if (m.isLiteMember()) {
                continue;
            }
            Version v = m.getVersion().asVersion();
            int currentCount;
            if (version0 == null || version0.equals(v)) {
                version0 = v;
                currentCount = ++count0;
            } else if (version1 == null || version1.equals(v)) {
                version1 = v;
                currentCount = ++count1;
            } else {
                throw new RuntimeException("More than 2 distinct member versions found: " + version0 + ", " + version1 + ", "
                        + v);
            }
            // a shortcut
            if (currentCount > grossMajority && localMember != null && localMember.getVersion().asVersion().equals(v)) {
                return localMember;
            }
        }

        assert count1 == 0 || count0 > 0;

        // no data members
        if (count0 == 0) {
            return null;
        }

        int count;
        Version version;
        if (count0 > count1 || count0 == count1 && version0.compareTo(version1) > 0) {
            count = count0;
            version = version0;
        } else {
            count = count1;
            version = version1;
        }

        // if the local member is data member and is from the larger group, use that
        if (localMember != null && !localMember.isLiteMember()
                && localMember.getVersion().asVersion().equals(version)) {
            return localMember;
        }

        // otherwise return a random member from the larger group
        int randomMemberIndex = ThreadLocalRandom.current().nextInt(count);
        for (Member m : members) {
            if (!m.isLiteMember() && m.getVersion().asVersion().equals(version)) {
                randomMemberIndex--;
                if (randomMemberIndex < 0) {
                    return m;
                }
            }
        }

        throw new RuntimeException("should never get here");
    }
}
