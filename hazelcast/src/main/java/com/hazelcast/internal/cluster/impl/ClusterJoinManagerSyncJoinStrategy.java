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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.OnJoinOp;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;
import static java.lang.String.format;

/**
 * mimics functionality when join is delayed and clients are blocked until a
 * certain timeout
 * Can be removed when this strategy is obsoleted
 *
 * @author lprimak
 */
public class ClusterJoinManagerSyncJoinStrategy {
    long timeToStartJoin;
    private final ClusterJoinManager clusterJoinManager;
    private final ILogger logger;
    private final long maxWaitMillisBeforeJoin;
    private final long waitMillisBeforeJoin;

    private long firstJoinRequest;

    ClusterJoinManagerSyncJoinStrategy(ClusterJoinManager clusterJoinManager, ILogger logger,
                                       long maxWaitMillisBeforeJoin, long waitMillisBeforeJoin) {
        this.clusterJoinManager = clusterJoinManager;
        this.logger = logger;
        this.maxWaitMillisBeforeJoin = maxWaitMillisBeforeJoin;
        this.waitMillisBeforeJoin = waitMillisBeforeJoin;
    }

    /**
     * Start processing the join request. This method is executed by the master node. In the case that there hasn't been any
     * previous join requests from the {@code memberInfo}'s address the master will first respond by sending the master answer.
     *
     * Also, during the first {@link ClusterProperty#MAX_WAIT_SECONDS_BEFORE_JOIN} period since the master received the first
     * join request from any node, the master will always wait for {@link ClusterProperty#WAIT_SECONDS_BEFORE_JOIN} before
     * allowing any join request to proceed. This means that in the initial period from receiving the first ever join request,
     * every new join request from a different address will prolong the wait time. After the initial period, join requests
     * will get processed as they arrive for the first time.
     *
     * @param memberInfo the joining member info
     * @param preJoinOperation which is prepared on joining members and will run on the master
     */
    void startJoinRequest(MemberInfo memberInfo, OnJoinOp preJoinOperation) {
        long now = Clock.currentTimeMillis();
        if (logger.isFineEnabled()) {
            String timeToStart = (timeToStartJoin > 0 ? ", timeToStart: " + (timeToStartJoin - now) : "");
            logger.fine(format("Handling join from %s, joinInProgress: %b%s", memberInfo.getAddress(),
                    clusterJoinManager.isJoinInProgress(), timeToStart));
        }

        if (firstJoinRequest == 0) {
            firstJoinRequest = now;
        }

        // Store the OnJoinOp passed in joiningMembers map to execute later; otherwise when we batch
        //  join request, only the final joiner's OnJoinOp is executed - we want to execute them all!
        final BiTuple<MemberInfo, OnJoinOp> existing = clusterJoinManager.joiningMembers.put(memberInfo.getAddress(),
                BiTuple.of(memberInfo, preJoinOperation));
        if (existing == null) {
            clusterJoinManager.sendMasterAnswer(memberInfo.getAddress());
            if (now - firstJoinRequest < maxWaitMillisBeforeJoin) {
                timeToStartJoin = now + waitMillisBeforeJoin;
            }
        } else if (!existing.element1().getUuid().equals(memberInfo.getUuid())) {
            logger.warning("Received a new join request from " + memberInfo.getAddress()
                    + " with a new UUID " + memberInfo.getUuid()
                    + ". Previous UUID was " + existing.element1().getUuid());
        }

        if (!clusterJoinManager.isBatchingJoins(now)) {
            clusterJoinManager.startJoin(memberInfo, preJoinOperation);
        }
    }

    void reset() {
        timeToStartJoin = Clock.currentTimeMillis() + waitMillisBeforeJoin;
        firstJoinRequest = 0;
    }
}
