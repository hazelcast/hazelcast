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
import com.hazelcast.internal.cluster.fd.PhiAccrualClusterFailureDetector;
import com.hazelcast.quorum.HeartbeatAware;
import com.hazelcast.quorum.QuorumFunction;
import com.hazelcast.util.Clock;

import java.util.Collection;

public class ProbabilisticQuorumFunction extends AbstractPingAwareQuorumFunction
        implements HeartbeatAware, QuorumFunction, MembershipListener {

    private final double suspicionThreshold;
    private final int quorumSize;
    private final int maxSampleSize;
    private final long minStdDeviationMillis;
    private final long acceptableHeartbeatPauseMillis;
    private final long heartbeatIntervalMillis;
    private final PhiAccrualClusterFailureDetector failureDetector;

    public ProbabilisticQuorumFunction(int quorumSize, long heartbeatIntervalMillis,
                                       long acceptableHeartbeatPauseMillis,
                                       int maxSampleSize,
                                       long minStdDeviationMillis,
                                       double suspicionThreshold) {
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.acceptableHeartbeatPauseMillis = acceptableHeartbeatPauseMillis;
        this.maxSampleSize = maxSampleSize;
        this.minStdDeviationMillis = minStdDeviationMillis;
        this.suspicionThreshold = suspicionThreshold;
        this.quorumSize = quorumSize;

        this.failureDetector = new PhiAccrualClusterFailureDetector(acceptableHeartbeatPauseMillis,
                heartbeatIntervalMillis, suspicionThreshold, maxSampleSize, minStdDeviationMillis);
    }

    @Override
    public boolean apply(Collection<Member> members) {
        if (members.size() < quorumSize) {
            return false;
        }

        int count = 0;
        long timestamp = Clock.currentTimeMillis();
        for (Member member : members) {
            if (!isAlivePerIcmp(member)) {
                continue;
            }

            if (member.localMember() || failureDetector.isAlive(member, timestamp)) {
                count++;
            }
        }
        return count >= quorumSize;
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        super.memberRemoved(membershipEvent);
        failureDetector.remove(membershipEvent.getMember());
    }

    @Override
    public void onHeartbeat(Member member, long timestamp) {
        failureDetector.heartbeat(member, timestamp);
    }

    public double getSuspicionThreshold() {
        return suspicionThreshold;
    }

    public int getMaxSampleSize() {
        return maxSampleSize;
    }

    public long getMinStdDeviationMillis() {
        return minStdDeviationMillis;
    }

    public long getAcceptableHeartbeatPauseMillis() {
        return acceptableHeartbeatPauseMillis;
    }

    public long getHeartbeatIntervalMillis() {
        return heartbeatIntervalMillis;
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProbabilisticQuorumFunction that = (ProbabilisticQuorumFunction) o;

        if (Double.compare(that.suspicionThreshold, suspicionThreshold) != 0) {
            return false;
        }
        if (quorumSize != that.quorumSize) {
            return false;
        }
        if (maxSampleSize != that.maxSampleSize) {
            return false;
        }
        if (minStdDeviationMillis != that.minStdDeviationMillis) {
            return false;
        }
        if (acceptableHeartbeatPauseMillis != that.acceptableHeartbeatPauseMillis) {
            return false;
        }
        return heartbeatIntervalMillis == that.heartbeatIntervalMillis;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(suspicionThreshold);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + quorumSize;
        result = 31 * result + maxSampleSize;
        result = 31 * result + (int) (minStdDeviationMillis ^ (minStdDeviationMillis >>> 32));
        result = 31 * result + (int) (acceptableHeartbeatPauseMillis ^ (acceptableHeartbeatPauseMillis >>> 32));
        result = 31 * result + (int) (heartbeatIntervalMillis ^ (heartbeatIntervalMillis >>> 32));
        return result;
    }
}
