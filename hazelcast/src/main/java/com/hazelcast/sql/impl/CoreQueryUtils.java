/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.version.Version;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class CoreQueryUtils {

    private CoreQueryUtils() {
    }

    public static HazelcastSqlException toPublicException(@Nonnull Throwable e, @Nonnull UUID localMemberId) {
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
            Throwable copy = e;
            while (copy != null) {
                if (ExceptionUtil.isTopologyException(copy)) {
                    return new HazelcastSqlException(localMemberId, SqlErrorCode.TOPOLOGY_CHANGE, e.getMessage(), e, null);
                } else if (copy instanceof RestartableException) {
                    return new HazelcastSqlException(localMemberId, SqlErrorCode.RESTARTABLE_ERROR, e.getMessage(), e, null);
                }
                copy = copy.getCause();
            }

            return new HazelcastSqlException(localMemberId, SqlErrorCode.GENERIC, e.getMessage(), e, null);
        }
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
     * @param members     list of all members
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
