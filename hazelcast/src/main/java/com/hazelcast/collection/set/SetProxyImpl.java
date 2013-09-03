package com.hazelcast.collection.set;

import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemListener;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Iterator;

/**
 * @ali 9/3/13
 */
public class SetProxyImpl<E> extends AbstractDistributedObject<SetService> implements ISet<E>, InitializingObject {

    private final String name;

    public SetProxyImpl(String name, NodeEngine nodeEngine, SetService service) {
        super(nodeEngine, service);
        this.name = name;
    }

    @Override
    public Object getId() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public String addItemListener(ItemListener<E> listener, boolean includeValue) {
        return null;
    }

    @Override
    public boolean removeItemListener(String registrationId) {
        return false;
    }

    @Override
    public void initialize() {

    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public boolean add(E e) {
        return false;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {

    }
}
