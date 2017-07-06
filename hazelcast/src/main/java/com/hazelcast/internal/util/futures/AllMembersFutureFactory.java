package com.hazelcast.internal.util.futures;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.String.format;

/**
 * Creates future per each member in a the cluster. Starting from the oldest to the youngest member.
 * It restarts on a cluster topology changes.
 *
 * @param <T>
 */
class AllMembersFutureFactory<T> {
    private final OperationFactory operationFactory;
    private final OperationService operationService;
    private final ClusterService clusterService;

    private final Queue<Member> memberQueue = new ConcurrentLinkedQueue<Member>();
    private final int maxRetries;

    private volatile Set<Member> initialMembers;

    //the retryCounter is accessed from multiple threads but never concurrently
    private volatile int retryCounter;

    public AllMembersFutureFactory(OperationFactory operationFactory,
                                   OperationService operationService, ClusterService clusterService, int maxRetries) {
        this.operationFactory = operationFactory;
        this.operationService = operationService;
        this.clusterService = clusterService;
        this.maxRetries = maxRetries;
    }

    public ICompletableFuture<T> createFuture() {
        Set<Member> members = clusterService.getMembers();
        if (initialMembers == null) {
            return start(members);
        }

        if (isClusterTopologyChanged(members)) {
            return retry(members);
        }

        Member nextMember = memberQueue.poll();
        if (nextMember != null) {
            return invokeOnMember(nextMember);
        }

        return null;
    }

    private ICompletableFuture<T> retry(Set<Member> currentMembers) {
        retryCounter++;
        if (retryCounter > maxRetries) {
            throw new HazelcastException(format("Cluster topology was not stable for %d retries,"
                    + " invoke on stable cluster failed", maxRetries));
        }
        return start(currentMembers);
    }

    private boolean isClusterTopologyChanged(Set<Member> currentMembers) {
        return !initialMembers.equals(currentMembers);
    }

    private ICompletableFuture<T> start(Set<Member> initialMembers) {
        this.initialMembers = initialMembers;
        memberQueue.clear();
        for (Member member : initialMembers) {
            memberQueue.add(member);
        }

        Member member = memberQueue.poll();
        assert member != null;

        return invokeOnMember(member);
    }

    private ICompletableFuture<T> invokeOnMember(Member member) {
        Address address = member.getAddress();
        Operation operation = operationFactory.createOperation();
        String serviceName = operation.getServiceName();
        InternalCompletableFuture<T> future = operationService.invokeOnTarget(serviceName, operation, address);
        return future;
    }
}
