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
import com.hazelcast.config.Config;
import com.hazelcast.config.IcmpFailureDetectorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.cluster.fd.PingFailureDetector;
import com.hazelcast.splitbrainprotection.PingAware;

import static com.hazelcast.config.ConfigAccessor.getActiveMemberNetworkConfig;

/**
 * Base class for split brain protection functions which may use ICMP failure detector information
 * to conclude whether the minimum cluster size property is satisfied.
 */
public abstract class AbstractPingAwareSplitBrainProtectionFunction implements PingAware, HazelcastInstanceAware,
        MembershipListener {
    private boolean pingFDEnabled;
    private PingFailureDetector<Member> pingFailureDetector;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        Config config = hazelcastInstance.getConfig();
        IcmpFailureDetectorConfig icmpFailureDetectorConfig = getActiveMemberNetworkConfig(config).getIcmpFailureDetectorConfig();
        boolean icmpEnabled = icmpFailureDetectorConfig != null && icmpFailureDetectorConfig.isEnabled();
        boolean icmpParallelMode = icmpEnabled && icmpFailureDetectorConfig.isParallelMode();

        // only take into account ping information if ICMP parallel mode is enabled
        if (!icmpEnabled || !icmpParallelMode) {
            return;
        }

        int icmpMaxAttempts = icmpFailureDetectorConfig.getMaxAttempts();
        this.pingFailureDetector = new PingFailureDetector<>(icmpMaxAttempts);
        this.pingFDEnabled = true;
    }

    @Override
    public void onPingLost(Member member) {
        if (!pingFDEnabled) {
            return;
        }
        pingFailureDetector.logAttempt(member);
    }

    @Override
    public void onPingRestored(Member member) {
        if (!pingFDEnabled) {
            return;
        }
        pingFailureDetector.heartbeat(member);
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        // ensure ping FD has heard at least once from each member
        if (pingFDEnabled) {
            pingFailureDetector.heartbeat(membershipEvent.getMember());
        }
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        if (pingFDEnabled) {
            pingFailureDetector.remove(membershipEvent.getMember());
        }
    }

    protected boolean isAlivePerIcmp(Member member) {
        if (!pingFDEnabled || member.localMember()) {
            return true;
        }

        return pingFailureDetector.isAlive(member);
    }
}
