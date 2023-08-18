/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 * Portions Copyright (c) 2020, MicroRaft.
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

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Mutable state maintained by the leader of the Raft group for each follower.
 * In follower state, three variables are stored:
 * <ul>
 * <li>{@code nextIndex}: index of the next log entry to send to that server
 * (initialized to leader's {@code lastLogIndex + 1})</li>
 * <li>{@code matchIndex}: index of highest log entry known to be replicated
 * on server (initialized to 0, increases monotonically)</li>
 * <li>{@code backoffRound}: denotes the current round in the ongoing backoff period</li>
 * <li>{@code nextBackoffPower}: used for calculating how many rounds will be used in the next backoff period</li>
 * <li>{@code appendRequestAckTimestamp}: the timestamp of the last append entries or install snapshot response</li>
 * <li>{@code flowControlSequenceNumber}: the flow control sequence number sent to the follower in the last append
 * entries or install snapshot request</li>
 * </ul>
 */
public class FollowerState {

    static final int MIN_BACKOFF_ROUNDS = 4;

    static final int MAX_BACKOFF_ROUND = 20;

    private long matchIndex;

    private long nextIndex;

    private int backoffRound;

    private int nextBackoffPower;

    private long appendRequestAckTimestamp;

    private long flowControlSequenceNumber;

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
     * Starts a new request backoff period. No new append entries or install
     * snapshot request will be sent to this follower either until it sends a
     * response or the backoff timeout elapses.
     * <p>
     * Returns the flow control sequence number to be put into the append entries or
     * install snapshot request which is to be sent to the follower.
     */
    public long setAppendRequestBackoff() {
        assert backoffRound == 0 : "backoff round: " + backoffRound;
        backoffRound = nextBackoffRound();
        return ++flowControlSequenceNumber;
    }

    private int nextBackoffRound() {
        return min(max((1 << (nextBackoffPower++)) * MIN_BACKOFF_ROUNDS, MIN_BACKOFF_ROUNDS), MAX_BACKOFF_ROUND);
    }

    /**
     * Sets the flag for append/install snapshot request backoff to max value.
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
        assert backoffRound > 0;
        return --backoffRound == 0;
    }

    /**
     * Updates the timestamp of the last received append entries or install snapshot
     * response. In addition, if the received flow control sequence number is equal
     * to the last sent flow sequence number, the internal request backoff state is
     * also reset.
     */
    public boolean appendRequestAckReceived(long flowControlSequenceNumber) {
        appendRequestAckTimestamp = Clock.currentTimeMillis();
        boolean success = this.flowControlSequenceNumber == flowControlSequenceNumber;

        // TODO RU_COMPAT_5_3 added for Version 5.3 compatibility. Should be removed at Version 5.5
        if (flowControlSequenceNumber == -1) {
            success = true;
        }

        if (success) {
            resetRequestBackoff();
        }

        return success;
    }

    /**
     * Resets the request backoff state.
     */
    public void resetRequestBackoff() {
        backoffRound = 0;
        nextBackoffPower = 0;
    }

    /**
     * Returns timestamp of the last append entries response
     */
    public long appendRequestAckTimestamp() {
        return appendRequestAckTimestamp;
    }

    int backoffRound() {
        return backoffRound;
    }

    public long flowControlSequenceNumber() {
        return flowControlSequenceNumber;
    }

    @Override
    public String toString() {
        return "FollowerState{" + "matchIndex=" + matchIndex + ", nextIndex=" + nextIndex + ", backoffRound=" + backoffRound
                + ", nextBackoffPower=" + nextBackoffPower + ", appendRequestAckTime=" + appendRequestAckTimestamp
                + ", flowControlSequenceNumber=" + flowControlSequenceNumber + '}';
    }
}
