package com.hazelcast.client.proxy;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.concurrent.atomicreference.client.*;
import com.hazelcast.core.Function;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;

import static com.hazelcast.util.ValidationUtil.isNotNull;

public class ClientAtomicReferenceProxy<E> extends ClientProxy implements IAtomicReference<E> {

    private final String name;
    private volatile Data key;

    public ClientAtomicReferenceProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
        this.name = objectId;
    }

    @Override
    public <R> R apply(Function<E, R> function) {
        isNotNull(function, "function");
        return invoke(new ApplyRequest(name, toData(function)));
    }

    @Override
    public void alter(Function<E, E> function) {
        isNotNull(function, "function");
        invoke(new AlterRequest(name, toData(function)));
    }

    @Override
    public E alterAndGet(Function<E, E> function) {
        isNotNull(function, "function");
        return invoke(new AlterAndGetRequest(name, toData(function)));
    }

    @Override
    public E getAndAlter(Function<E, E> function) {
        isNotNull(function, "function");
        return invoke(new GetAndAlterRequest(name, toData(function)));
    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        return invoke(new CompareAndSetRequest(name, toData(expect), toData(update)));
    }

    @Override
    public boolean contains(E expected) {
        return invoke(new ContainsRequest(name, toData(expected)));
    }

    @Override
    public E get() {
        return invoke(new GetRequest(name));
    }

    @Override
    public void set(E newValue) {
        invoke(new SetRequest(name, toData(newValue)));
    }

    @Override
    public void clear() {
        set(null);
    }

    @Override
    public E getAndSet(E newValue) {
        return invoke(new GetAndSetRequest(name, toData(newValue)));
    }

    @Override
    public E setAndGet(E update) {
        invoke(new SetRequest(name, toData(update)));
        return update;
    }

    @Override
    public boolean isNull() {
        return invoke(new IsNullRequest(name));
    }

    @Override
    protected void onDestroy() {
    }

    private <T> T invoke(Object req) {
        try {
            return getContext().getInvocationService().invokeOnKeyOwner(req, getKey());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Data getKey() {
        if (key == null) {
            key = toData(name);
        }
        return key;
    }

    private Data toData(Object object) {
        return getContext().getSerializationService().toData(object);
    }
}

