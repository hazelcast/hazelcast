/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.iterator;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterTopologyChangedException;
import com.hazelcast.internal.util.futures.ChainingFuture;
import com.hazelcast.spi.exception.TargetNotMemberException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.String.format;


/**
 * Iterates over stable cluster from the oldest to youngest member.
 * It restarts when detects a cluster topology change.
 *
 * It can be used from multiple threads, but not concurrently.
 *
 */
public class RestartingMemberIterator implements Iterator<Member>, ChainingFuture.ExceptionHandler {

    private final ClusterService clusterService;
    private final Queue<Member> memberQueue = new ConcurrentLinkedQueue<Member>();
    private final int maxRetries;

    private volatile Set<Member> initialMembers;
    private volatile Member nextMember;
    private volatile int retryCounter;
    private volatile boolean topologyChanged;

    public RestartingMemberIterator(ClusterService clusterService, int maxRetries) {
        this.clusterService = clusterService;
        this.maxRetries = maxRetries;

        Set<Member> currentMembers = clusterService.getMembers();
        startNewRound(currentMembers);
    }

    private void startNewRound(Set<Member> currentMembers) {
        topologyChanged = false;
        for (Member member : currentMembers) {
            memberQueue.add(member);
        }
        nextMember = memberQueue.poll();
        this.initialMembers = currentMembers;
    }

    @Override
    public boolean hasNext() {
        if (nextMember != null) {
            return true;
        }
        return advance();
    }

    private boolean advance() {
        Set<Member> currentMembers = clusterService.getMembers();
        if (topologyChanged(currentMembers)) {
            retry(currentMembers);
            // at any given moment there should always be at least 1 cluster member (our own member)
            assert nextMember != null;
            return true;
        }

        nextMember = memberQueue.poll();
        return nextMember != null;
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "retryCounter is accessed by multiple threads, but never concurrently")
    private void retry(Set<Member> currentMembers) {
        retryCounter++;
        if (retryCounter > maxRetries) {
            throw new HazelcastException(format("Cluster topology was not stable for %d retries,"
                    + " invoke on stable cluster failed", maxRetries));
        }
        memberQueue.clear();
        startNewRound(currentMembers);
    }

    private boolean topologyChanged(Set<Member> currentMembers) {
        return topologyChanged || !currentMembers.equals(initialMembers);
    }

    @Override
    public Member next() {
        Member memberToReturn = nextMember;
        nextMember = null;
        if (memberToReturn != null) {
            return memberToReturn;
        }
        if (!advance()) {
            throw new NoSuchElementException("no more elements");
        }
        memberToReturn = nextMember;
        nextMember = null;
        return memberToReturn;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public <T extends Throwable> void handle(T throwable)
            throws T {
        if (throwable instanceof ClusterTopologyChangedException) {
            topologyChanged = true;
            return;
        }

        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException
                || throwable instanceof HazelcastInstanceNotActiveException) {
            return;
        }
        throw throwable;
    }

    public int getRetryCount() {
        return retryCounter;
    }
}
