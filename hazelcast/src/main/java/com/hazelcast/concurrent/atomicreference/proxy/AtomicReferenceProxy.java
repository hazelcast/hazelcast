package com.hazelcast.concurrent.atomicreference.proxy;

import com.hazelcast.concurrent.atomicreference.*;
import com.hazelcast.core.Function;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

import static com.hazelcast.util.ValidationUtil.isNotNull;

public class AtomicReferenceProxy<E> extends AbstractDistributedObject<AtomicReferenceService> implements IAtomicReference<E> {

    private final String name;
    private final int partitionId;

    public AtomicReferenceProxy(String name, NodeEngine nodeEngine, AtomicReferenceService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    private <E> E invoke(Operation operation, NodeEngine nodeEngine) {
        try {
            Invocation inv = nodeEngine
                    .getOperationService()
                    .createInvocationBuilder(AtomicReferenceService.SERVICE_NAME, operation, partitionId)
                    .build();
            Future<Data> f = inv.invoke();
            return nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public void alter(Function<E, E> function) {
        isNotNull(function,"function");

        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new AlterOperation(name, nodeEngine.toData(function));
        invoke(operation,nodeEngine);
    }

    @Override
    public E alterAndGet(Function<E, E> function) {
        isNotNull(function,"function");

        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new AlterAndGetOperation(name, nodeEngine.toData(function));
        return invoke(operation,nodeEngine);
    }

    @Override
    public E getAndAlter(Function<E, E> function) {
        isNotNull(function,"function");

        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new GetAndAlterOperation(name, nodeEngine.toData(function));
        return invoke(operation,nodeEngine);
    }

    @Override
    public <R> R apply(Function<E, R> function) {
        isNotNull(function,"function");

        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new ApplyOperation(name, nodeEngine.toData(function));
        return invoke(operation,nodeEngine);
    }

    @Override
    public void clear() {
        set(null);
    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new CompareAndSetOperation(name, nodeEngine.toData(expect), nodeEngine.toData(update));
        return invoke(operation,nodeEngine);
    }

    @Override
    public E get() {
        Operation operation = new GetOperation(name);
        return invoke(operation,getNodeEngine());
    }

    @Override
    public boolean contains(E expected) {
        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new ContainsOperation(name,nodeEngine.toData(expected));
        return invoke(operation,nodeEngine);
    }

    @Override
    public void set(E newValue) {
        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new SetOperation(name, nodeEngine.toData(newValue));
        invoke(operation,nodeEngine);
    }

    @Override
    public E getAndSet(E newValue) {
        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new GetAndSetOperation(name, nodeEngine.toData(newValue));
        return invoke(operation,nodeEngine);
    }

    @Override
    public E setAndGet(E newValue) {
        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new SetOperation(name, nodeEngine.toData(newValue));
        invoke(operation,nodeEngine);
        return newValue;
    }

    @Override
    public boolean isNull() {
        Operation operation = new IsNullOperation(name);
        return invoke(operation,getNodeEngine());
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
