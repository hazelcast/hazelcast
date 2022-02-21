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

import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * This class is used to keep the queries until a heartbeat round is performed
 * for running queries without appending entries to the Raft log. These queries
 * still achieve linearizability.
 * <p>
 * Section 6.4 of Raft Dissertation:
 * ...
 * Linearizability requires the results of a read to reflect a state of the
 * system sometime after the read was initiated; each read must at least return
 * the results of the latest committed write.
 * ...
 * Fortunately, it is possible to bypass the Raft log for read-only queries and
 * still preserve linearizability.
 */
public class QueryState {

    /**
     * The minimum commit index required on the leader to execute the queries.
     */
    private long queryCommitIndex;

    /**
     * The index of the heartbeat round to execute the currently waiting
     * queries. When a query is received and there is no other query waiting
     * to be executed, a new heartbeat round is started by incrementing this
     * field.
     * <p>
     * Value of this field is put into AppendEntriesRPCs sent to followers and
     * bounced back to the leader to complete the heartbeat round and execute
     * the queries.
     */
    private long queryRound;

    /**
     * Queries waiting to be executed.
     */
    private final List<BiTuple<Object, InternalCompletableFuture>> operations = new ArrayList<>();

    /**
     * The set of followers acknowledged the leader in the current heartbeat
     * round that is specified by {@link #queryRound}.
     */
    private final Set<RaftEndpoint> acks = new HashSet<>();

    /**
     * Adds the given query to the collection of queries and returns the number
     * of queries waiting to be executed. Also updates the minimum commit index
     * that is expected on the leader to execute the queries.
     */
    public int addQuery(long commitIndex, Object operation, InternalCompletableFuture resultFuture) {
        if (commitIndex < queryCommitIndex) {
            throw new IllegalArgumentException("Cannot execute query: " + operation + " at commit index because of the current "
                    + this);
        }

        if (queryCommitIndex < commitIndex) {
            queryCommitIndex = commitIndex;
        }

        operations.add(BiTuple.of(operation, resultFuture));
        int size = operations.size();
        if (size == 1) {
            queryRound++;
        }

        return size;
    }

    /**
     * Returns {@code true} if the given follower is accepted as an acker
     * for the current query round. It is accepted only if there are
     * waiting queries to be executed and the {@code queryRound} argument
     * matches to the current query round.
     */
    public boolean tryAck(long queryRound, RaftEndpoint follower) {
        // If there is no query waiting to be executed or the received ack
        // belongs to an earlier query round, we ignore it.
        if (queryCount() == 0  || this.queryRound > queryRound) {
            return false;
        }

        checkTrue(queryRound == this.queryRound, this + ", acked query round: "
                + queryRound + ", follower: " + follower);

        return acks.add(follower);
    }

    /**
     * Returns {@code true} if the given follower is removed from the ack list.
     */
    public boolean removeAck(RaftEndpoint follower) {
        return acks.remove(follower);
    }

    /**
     * Returns the number of queries waiting for execution.
     */
    public int queryCount() {
        return operations.size();
    }

    /**
     * Returns the index of the heartbeat round to execute the currently
     * waiting queries.
     */
    public long queryRound() {
        return queryRound;
    }

    /**
     * Returns {@code true} if there are queries waiting and acks are received
     * from the majority. Fails with {@link IllegalStateException} if
     * the given commit index is smaller than {@link #queryCommitIndex}.
     */
    public boolean isMajorityAcked(long commitIndex, int majority) {
        if (queryCommitIndex > commitIndex) {
            throw new IllegalStateException("Cannot execute: " + this + ", current commit index: " + commitIndex);
        }

        return queryCount() > 0 && majority <= ackCount();
    }

    public boolean isAckNeeded(RaftEndpoint follower, int majority) {
        return queryCount() > 0 && !acks.contains(follower) && ackCount() < majority;
    }

    private int ackCount() {
        return acks.size() + 1;
    }

    /**
     * Returns the queries waiting to be executed.
     */
    public Collection<BiTuple<Object, InternalCompletableFuture>> operations() {
        return operations;
    }

    /**
     * Resets the collection of waiting queries and acknowledged followers.
     */
    public void reset() {
        operations.clear();
        acks.clear();
    }

    @Override
    public String toString() {
        return "QueryState{" + "queryCommitIndex=" + queryCommitIndex + ", queryRound=" + queryRound + ", queryCount="
                + queryCount() + ", acks=" + acks + '}';
    }
}
