/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum.impl;

import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.quorum.HeartbeatAware;
import com.hazelcast.quorum.QuorumFunction;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.Clock.currentTimeMillis;

/**
 * A quorum function that keeps track of the last heartbeat timestamp per member. For a member to be
 * considered live (for the purpose of determining presence of quorum), a heartbeat must have
 * been received at most {@code heartbeatToleranceMillis} milliseconds before current time.
 */
public class RecentlyActiveQuorumFunction extends AbstractPingAwareQuorumFunction
        implements HeartbeatAware, QuorumFunction, MembershipListener {

    private final int quorumSize;
    private final int heartbeatToleranceMillis;
    private final ConcurrentMap<Member, Long> latestHeartbeatPerMember = new ConcurrentHashMap<Member, Long>();

    public RecentlyActiveQuorumFunction(int quorumSize, int heartbeatToleranceMillis) {
        this.quorumSize = quorumSize;
        this.heartbeatToleranceMillis = heartbeatToleranceMillis;
    }

    @Override
    public boolean apply(Collection<Member> members) {
        if (members.size() < quorumSize) {
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
        return count >= quorumSize;
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
        if (!(o instanceof RecentlyActiveQuorumFunction)) {
            return false;
        }

        RecentlyActiveQuorumFunction that = (RecentlyActiveQuorumFunction) o;

        if (quorumSize != that.quorumSize) {
            return false;
        }
        return heartbeatToleranceMillis == that.heartbeatToleranceMillis;
    }

    @Override
    public int hashCode() {
        int result = quorumSize;
        result = 31 * result + heartbeatToleranceMillis;
        return result;
    }
}
