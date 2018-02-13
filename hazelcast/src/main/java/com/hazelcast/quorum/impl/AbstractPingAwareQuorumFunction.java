/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.fd.PingFailureDetector;
import com.hazelcast.quorum.PingAware;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

/**
 * Base class for quorum functions which may use ICMP failure detector information to determine
 * presence of quorum.
 */
public class AbstractPingAwareQuorumFunction implements PingAware, HazelcastInstanceAware {
    protected boolean pingFDEnabled;
    protected PingFailureDetector<Member> pingFailureDetector;
    protected Cluster cluster;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        Config config = hazelcastInstance.getConfig();
        IcmpFailureDetectorConfig icmpFailureDetectorConfig = config.getNetworkConfig().getIcmpFailureDetectorConfig();
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
        this.cluster = hazelcastInstance.getCluster();
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

    protected boolean isAlivePerIcmp(Member member) {
        if (!pingFDEnabled) {
            return true;
        }
        if (cluster.getLocalMember().equals(member)) {
            return true;
        }
        return pingFailureDetector.isAlive(member);
    }
}
