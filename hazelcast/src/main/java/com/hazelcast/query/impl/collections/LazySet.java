/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.collections;

import com.hazelcast.util.function.BiConsumer;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Lazy Set implementation for holding query results.
 * Support deferred operations which can be called after initializing the delegate Set
 */
public abstract class LazySet<E> implements Set<E> {

    /**
     * List of deferred operations. Will be called automatically after delegate collection created.
     */
    private List<BiConsumer<Long, Integer>> deferredOperations;

    private Set<E> delegate;

    /**
     * Initialize the delegate.
     *
     * @return the delegate.
     */
    @Nonnull
    protected abstract Set<E> initialize();

    /**
     * Gets the delegate.
     *
     * @return the delegate.
     */
    @Nonnull
    private synchronized Set<E> delegate() {
        if (delegate == null) {
            long start = System.nanoTime();
            delegate = initialize();
            doDeferredOperations(System.nanoTime() - start);
        }
        return delegate;
    }


    /**
     * Instantiates and return the delegate
     * @return the delegate
     */
    @Nonnull
    public Set<E> getDelegate() {
        return delegate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return delegate().size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return delegate().isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(Object o) {
        return delegate().contains(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<E> iterator() {
        return delegate().iterator();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] toArray() {
        return delegate().toArray();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T[] toArray(T[] a) {
        return delegate().toArray(a);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(E e) {
        return delegate().add(e);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(Object o) {
        return delegate().remove(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate().containsAll(c);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addAll(Collection<? extends E> c) {
        return delegate().addAll(c);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        return delegate().retainAll(c);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeAll(Collection<?> c) {
        return delegate().removeAll(c);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        delegate().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        return delegate().equals(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return delegate().hashCode();
    }

    /**
     * @return estimated size of the lazy collection without initializing the delegated collection
     */
    public abstract int estimatedSize();


    /**
     * add a deferred operation which will be invoked if this Lazy Set initialized
     * @param op
     */
    final void addDeferredOperation(BiConsumer<Long, Integer> op) {
        if (deferredOperations == null) {
            deferredOperations = new LinkedList<BiConsumer<Long, Integer>>();
        }
        deferredOperations.add(op);
    }


    private void doDeferredOperations(long elapsed) {
        if (deferredOperations != null) {
            for (BiConsumer<Long, Integer> opr : deferredOperations) {
                opr.accept(System.nanoTime() - elapsed, size());
            }
        }
    }

}
