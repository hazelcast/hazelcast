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

/**
 * Failure detector tracks heartbeats of a member and decides liveness/availability of the member.
 */
public interface FailureDetector {

    /**
     * Notifies this failure detector about received heartbeat message from the tracked member.
     *
     * @param timestamp timestamp of heartbeat message in milliseconds
     */
    void heartbeat(long timestamp);

    /**
     * Returns true if the tracked member is considered as alive/available.
     * @param timestamp timestamp in milliseconds
     * @return true if the member is alive
     */
    boolean isAlive(long timestamp);

    /**
     * Returns the last heartbeat timestamp for the tracked member.
     * @return heartbeat timestamp in milliseconds
     */
    long lastHeartbeat();

    /**
     * Returns suspicion level about the tracked member. Returned value is mostly implementation dependent.
     * <code>0</code> indicates no suspicion at all.
     * @param timestamp timestamp in milliseconds
     * @return suspicion level
     */
    double suspicionLevel(long timestamp);
}
