package com.hazelcast.concurrent.atomicreference.proxy;

import com.hazelcast.concurrent.atomicreference.*;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

public class AtomicReferenceProxy<E> extends AbstractDistributedObject<AtomicReferenceService> implements IAtomicReference<E> {

    private final String name;
    private final int partitionId;

    public AtomicReferenceProxy(String name, NodeEngine nodeEngine, AtomicReferenceService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        final NodeEngine nodeEngine = getNodeEngine();

        try {
            CompareAndSetOperation operation = new CompareAndSetOperation(name, nodeEngine.toData(expect), nodeEngine.toData(update));
            Invocation inv = newInvocation(operation);
            Future<Data> f = inv.invoke();
            return nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    private Invocation newInvocation(Operation operation) {
        return getNodeEngine().getOperationService().createInvocationBuilder(AtomicReferenceService.SERVICE_NAME, operation, partitionId).build();
    }

    @Override
    public E get() {
        final NodeEngine nodeEngine = getNodeEngine();

        try {
            GetOperation operation = new GetOperation(name);
            Invocation inv = newInvocation(operation);
            Future<Data> f = inv.invoke();
            return nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public void set(E newValue) {
        final NodeEngine nodeEngine = getNodeEngine();

        try {
            SetOperation operation = new SetOperation(name, nodeEngine.toData(newValue));
            Invocation inv = newInvocation(operation);
            Future<Data> f = inv.invoke();
            f.get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public E getAndSet(E newValue) {
        final NodeEngine nodeEngine = getNodeEngine();

        try {
            GetAndSetOperation operation = new GetAndSetOperation(name, nodeEngine.toData(newValue));
            Invocation inv = newInvocation(operation);
            Future<Data> f = inv.invoke();
            return nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public E setAndGet(E newValue) {
        final NodeEngine nodeEngine = getNodeEngine();

        try {
            SetOperation operation = new SetOperation(name, nodeEngine.toData(newValue));
            Invocation inv = newInvocation(operation);
            inv.invoke().get();
            return newValue;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public boolean isNull() {
        try {
            IsNullOperation operation = new IsNullOperation(name);
            Invocation inv = newInvocation(operation);
            return (Boolean) inv.invoke().get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String getServiceName() {
        return AtomicReferenceService.SERVICE_NAME;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IAtomicReference{");
        sb.append("name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
