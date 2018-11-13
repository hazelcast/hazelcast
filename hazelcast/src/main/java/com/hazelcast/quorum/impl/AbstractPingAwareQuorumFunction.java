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

import com.hazelcast.config.Config;
import com.hazelcast.config.IcmpFailureDetectorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.internal.cluster.fd.PingFailureDetector;
import com.hazelcast.quorum.PingAware;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import static com.hazelcast.config.ConfigAccessor.getActiveMemberNetworkConfig;

/**
 * Base class for quorum functions which may use ICMP failure detector information to determine
 * presence of quorum.
 */
public abstract class AbstractPingAwareQuorumFunction implements PingAware, HazelcastInstanceAware, MembershipListener {
    private boolean pingFDEnabled;
    private PingFailureDetector<Member> pingFailureDetector;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        Config config = hazelcastInstance.getConfig();
        IcmpFailureDetectorConfig icmpFailureDetectorConfig = getActiveMemberNetworkConfig(config).getIcmpFailureDetectorConfig();
        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);
        boolean icmpEnabled = icmpFailureDetectorConfig == null
                ? hazelcastProperties.getBoolean(GroupProperty.ICMP_ENABLED)
                : icmpFailureDetectorConfig.isEnabled();
        boolean icmpParallelMode = icmpEnabled && (icmpFailureDetectorConfig == null
                ? hazelcastProperties.getBoolean(GroupProperty.ICMP_PARALLEL_MODE)
                : icmpFailureDetectorConfig.isParallelMode());

        // only take into account ping information if ICMP parallel mode is enabled
        if (!icmpEnabled || !icmpParallelMode) {
            return;
        }

        int icmpMaxAttempts = icmpFailureDetectorConfig == null
                ? hazelcastProperties.getInteger(GroupProperty.ICMP_MAX_ATTEMPTS)
                : icmpFailureDetectorConfig.getMaxAttempts();
        this.pingFailureDetector = new PingFailureDetector<Member>(icmpMaxAttempts);
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
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        // quorum not affected by member attribute change
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
