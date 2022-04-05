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

package com.hazelcast.splitbrainprotection;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.cluster.fd.PingFailureDetector;

/**
 * Split brain protection functions that need access to ICMP ping failure detector should implement this interface;
 * the instance of {@link PingFailureDetector} used by this member's
 * {@link com.hazelcast.internal.cluster.impl.ClusterHeartbeatManager} will be provided on initialization.
 */
public interface PingAware {

    /**
     * Notifies the {@link SplitBrainProtectionFunction} of ping loss.
     *
     * @param member member which was not pinged successfully
     */
    void onPingLost(Member member);

    /**
     * Notifies the {@link SplitBrainProtectionFunction} of a successful ping after one or more pings to that member were lost.
     *
     * @param member member which was pinged successfully
     */
    void onPingRestored(Member member);
}
