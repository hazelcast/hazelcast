package com.hazelcast.client.proxy;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.concurrent.atomiclong.client.*;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;

public class ClientAtomicReferenceProxy<E>  extends ClientProxy implements IAtomicReference<E> {

    private final String name;
    private volatile Data key;

    public ClientAtomicReferenceProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
        this.name = objectId;
    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E get() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(E newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E getAndSet(E newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E setAndGet(E update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull() {
        throw new UnsupportedOperationException();
    }

    protected void onDestroy() {
    }

    private <T> T invoke(Object req){
        try {
            return getContext().getInvocationService().invokeOnKeyOwner(req, getKey());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Data getKey(){
        if (key == null){
            key = getContext().getSerializationService().toData(name);
        }
        return key;
    }
}

