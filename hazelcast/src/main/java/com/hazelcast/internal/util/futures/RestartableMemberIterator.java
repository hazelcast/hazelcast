package com.hazelcast.internal.util.futures;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;

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
public class RestartableMemberIterator implements Iterator<Member> {

    private final ClusterService clusterService;
    private final Queue<Member> memberQueue = new ConcurrentLinkedQueue<Member>();
    private final int maxRetries;

    private volatile Set<Member> initialMembers;
    private volatile Member nextMember;
    private volatile int retryCounter;


    public RestartableMemberIterator(ClusterService clusterService, int maxRetries) {
        this.clusterService = clusterService;
        this.maxRetries = maxRetries;

        Set<Member> members = clusterService.getMembers();
        startNewRound(members);
    }

    private void startNewRound(Set<Member> members) {
        memberQueue.clear();
        for (Member member : members) {
            memberQueue.add(member);
        }
        nextMember = memberQueue.poll();
        this.initialMembers = members;
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
            assert nextMember != null;
            return true;
        }

        nextMember = memberQueue.poll();
        return nextMember != null;
    }

    private void retry(Set<Member> currentMembers) {
        retryCounter++;
        if (retryCounter > maxRetries) {
            throw new HazelcastException(format("Cluster topology was not stable for %d retries,"
                    + " invoke on stable cluster failed", maxRetries));
        }
        startNewRound(currentMembers);
    }

    private boolean topologyChanged(Set<Member> currentMembers) {
        return !currentMembers.equals(initialMembers);
    }

    @Override
    public Member next() {
        Member memberToReturn = nextMember;
        nextMember = null;
        if (memberToReturn != null) {
            return memberToReturn;
        }
        advance();
        if (nextMember == null) {
            throw new NoSuchElementException("no more elements");
        }
        return nextMember;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("not implemented");
    }
}
