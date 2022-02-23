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

package com.hazelcast.internal.cluster.fd;

import com.hazelcast.cluster.Member;

/**
 * Cluster failure detector tracks heartbeats of the members and decides liveness/availability of them.
 */
public interface ClusterFailureDetector {

    /**
     * Notifies this failure detector about received heartbeat message from a member.
     *
     * @param member member which heartbeat message is received from
     * @param timestamp timestamp of heartbeat message in milliseconds
     */
    void heartbeat(Member member, long timestamp);

    /**
     * Returns true if given member is considered as alive/available.
     * @param member member whose liveness is questioned
     * @param timestamp timestamp in milliseconds
     * @return true if member is alive
     */
    boolean isAlive(Member member, long timestamp);

    /**
     * Returns the last heartbeat timestamp for a member.
     * @param member member whose heartbeat timestamp is requested
     * @return heartbeat timestamp in milliseconds
     */
    long lastHeartbeat(Member member);

    /**
     * Returns suspicion level about a given member. Returned value is mostly implementation dependent.
     * <code>0</code> indicates no suspicion at all.
     * @param member member
     * @param timestamp timestamp in milliseconds
     * @return suspicion level
     */
    double suspicionLevel(Member member, long timestamp);

    /**
     * Deregister member from tracking and cleanup resources related to this member.
     * @param member member to be removed
     */
    void remove(Member member);

    /**
     * Clear all state kept by this failure detector
     */
    void reset();

}
