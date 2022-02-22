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

package com.hazelcast.cp.internal.raft.impl.state;

import com.hazelcast.internal.util.Clock;

import static java.lang.Math.min;

/**
 * Mutable state maintained by the leader of the Raft group for each follower.
 * In follower state, three variables are stored:
 * <ul>
 * <li>{@code nextIndex}: index of the next log entry to send to that server
 * (initialized to leader's {@code lastLogIndex + 1})</li>
 * <li>{@code matchIndex}: index of highest log entry known to be replicated
 * on server (initialized to 0, increases monotonically)</li>
 * <li>{@code appendRequestBackoff}: a boolean flag indicating that leader is still
 * waiting for a response to the last sent append request</li>
 * </ul>
 */
public class FollowerState {

    private static final int MAX_BACKOFF_ROUND = 20;

    private long matchIndex;

    private long nextIndex;

    private int backoffRound;

    private int nextBackoffPower;

    private long appendRequestAckTimestamp;

    FollowerState(long matchIndex, long nextIndex) {
        this.matchIndex = matchIndex;
        this.nextIndex = nextIndex;
        this.appendRequestAckTimestamp = Clock.currentTimeMillis();
    }

    /**
     * Returns the match index for follower.
     */
    public long matchIndex() {
        return matchIndex;
    }

    /**
     * Sets the match index for follower.
     */
    public void matchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    /**
     * Returns the next index for follower.
     */
    public long nextIndex() {
        return nextIndex;
    }

    /**
     * Sets the next index for follower.
     */
    public void nextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    /**
     * Returns whether leader is waiting for response of the last append request.
     */
    public boolean isAppendRequestBackoffSet() {
        return backoffRound > 0;
    }

    /**
     * Sets the flag for append request backoff. A new append request will not be sent
     * to this follower either until it sends an append response or a backoff timeout occurs.
     */
    public void setAppendRequestBackoff() {
        backoffRound = nextBackoffRound();
        nextBackoffPower++;
    }

    private int nextBackoffRound() {
        return min(1 << nextBackoffPower, MAX_BACKOFF_ROUND);
    }

    /**
     * Sets the flag for append request backoff to max value.
     */
    public void setMaxAppendRequestBackoff() {
        backoffRound = MAX_BACKOFF_ROUND;
    }

    /**
     * Completes a single round of append request backoff.
     *
     * @return true if round number reaches to {@code 0}, false otherwise
     */
    public boolean completeAppendRequestBackoffRound() {
        return --backoffRound == 0;
    }

    /**
     * Clears the flag for the append request backoff
     * and updates the timestamp of append entries response
     */
    public void appendRequestAckReceived() {
        backoffRound = 0;
        nextBackoffPower = 0;
        appendRequestAckTimestamp = Clock.currentTimeMillis();
    }

    /**
     * Returns timestamp of the last append entries response
     */
    public long appendRequestAckTimestamp() {
        return appendRequestAckTimestamp;
    }

    @Override
    public String toString() {
        return "FollowerState{" + "matchIndex=" + matchIndex + ", nextIndex=" + nextIndex + ", backoffRound=" + backoffRound
                + ", nextBackoffPower=" + nextBackoffPower + ", appendRequestAckTime=" + appendRequestAckTimestamp + '}';
    }
}
