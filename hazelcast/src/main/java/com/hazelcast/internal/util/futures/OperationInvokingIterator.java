package com.hazelcast.internal.util.futures;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.Iterator;

/**
 * Invoke operation on each member provided by the member iterator
 *
 * @param <T>
 */
public class OperationInvokingIterator<T> implements Iterator<ICompletableFuture<T>> {
    private final OperationFactory operationFactory;
    private final OperationService operationService;
    private final Iterator<Member> memberIterator;


    //the retryCounter is accessed from multiple threads but never concurrently
    private volatile int retryCounter;

    public OperationInvokingIterator(Iterator<Member> memberIterator, OperationFactory operationFactory,
                                     OperationService operationService) {
        this.operationFactory = operationFactory;
        this.operationService = operationService;
        this.memberIterator = memberIterator;
    }

    @Override
    public boolean hasNext() {
        return memberIterator.hasNext();
    }

    @Override
    public ICompletableFuture<T> next() {
        Member member = memberIterator.next();
        return invokeOnMember(member);
    }

    private ICompletableFuture<T> invokeOnMember(Member member) {
        Address address = member.getAddress();
        Operation operation = operationFactory.createOperation();
        String serviceName = operation.getServiceName();
        InternalCompletableFuture<T> future = operationService.invokeOnTarget(serviceName, operation, address);
        return future;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("not implemented");
    }
}
