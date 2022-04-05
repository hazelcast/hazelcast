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

package com.hazelcast.splitbrainprotection.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.splitbrainprotection.HeartbeatAware;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.Clock.currentTimeMillis;

/**
 * A split brain protection function that keeps track of the last heartbeat timestamp per member. For a member to be
 * considered live (for the purpose to conclude whether the minimum cluster size property is satisfied),
 * a heartbeat must have been received at most {@code heartbeatToleranceMillis} milliseconds before current time.
 */
public class RecentlyActiveSplitBrainProtectionFunction extends AbstractPingAwareSplitBrainProtectionFunction
        implements HeartbeatAware, SplitBrainProtectionFunction, MembershipListener {

    private final int minimumClusterSize;
    private final int heartbeatToleranceMillis;
    private final ConcurrentMap<Member, Long> latestHeartbeatPerMember = new ConcurrentHashMap<Member, Long>();

    public RecentlyActiveSplitBrainProtectionFunction(int minimumClusterSize, int heartbeatToleranceMillis) {
        this.minimumClusterSize = minimumClusterSize;
        this.heartbeatToleranceMillis = heartbeatToleranceMillis;
    }

    @Override
    public boolean apply(Collection<Member> members) {
        if (members.size() < minimumClusterSize) {
            return false;
        }

        int count = 0;
        long now = currentTimeMillis();
        for (Member member : members) {
            if (!isAlivePerIcmp(member)) {
                continue;
            }

            if (member.localMember()) {
                count++;
                continue;
            }

            // apply and onHeartbeat are never executed concurrently
            Long latestTimestamp = latestHeartbeatPerMember.get(member);
            if (latestTimestamp == null) {
                continue;
            }

            if ((now - latestTimestamp) < heartbeatToleranceMillis) {
                count++;
            }
        }
        return count >= minimumClusterSize;
    }

    @Override
    public void onHeartbeat(Member member, long timestamp) {
        // apply and onHeartbeat are never executed concurrently
        latestHeartbeatPerMember.put(member, timestamp);
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        super.memberRemoved(membershipEvent);
        latestHeartbeatPerMember.remove(membershipEvent.getMember());
    }

    public int getHeartbeatToleranceMillis() {
        return heartbeatToleranceMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecentlyActiveSplitBrainProtectionFunction)) {
            return false;
        }

        RecentlyActiveSplitBrainProtectionFunction that = (RecentlyActiveSplitBrainProtectionFunction) o;

        if (minimumClusterSize != that.minimumClusterSize) {
            return false;
        }
        return heartbeatToleranceMillis == that.heartbeatToleranceMillis;
    }

    @Override
    public int hashCode() {
        int result = minimumClusterSize;
        result = 31 * result + heartbeatToleranceMillis;
        return result;
    }
}
