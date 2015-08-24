/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.collection;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Provides fast {@link Set} implementation for cases where items are known to not
 * contain duplicates.
 *
 * It doesn't not call equals/hash methods on initial data insertion hence it avoids
 * performance penalty in the case these methods are expensive. It also means it does
 * not detect duplicates - it's a responsibility of a caller to make sure no duplicated
 * entries are inserted.
 *
 * Once the initial load is done then caller should indicate it
 * by calling {@link #close()}. It switches operational mode and if any item is
 * inserted afterward then the Set will inflate - it copies its content into
 * internal HashSet. This means duplicate detection is enabled, but obviously
 * it ruins the initial performance gain. We are making a bet the Set will not be
 * modified once it's closed. The Set will also inflate on {@link #contains(Object)}
 * and {@link #containsAll(Collection)} calls to enable fast look-ups.
 *
 * Failing to call {@link #close()} means the InflatableSet is in state where
 * a general contract of Set is broken - duplicates aren't detected. You should
 * never pass unclosed InfltableSet to user code.
 *
 * It's intended to be use in cases where a contract mandates us to return Set,
 * but we know our data contains not duplicates. It performs the best in cases
 * biased towards sequential iterations.
 *
 *
 * @param <T>
 */
public final class InflatableSet<T> extends AbstractSet<T> implements Set<T>, Serializable, Cloneable {
    private static final long serialVersionUID = 0L;

    private enum State {
        //Set is still open for initial load. It's caller's responsibility to make
        //sure no duplicates are added to this Set in this state
        INITIAL_LOAD,

        //Only array-backed representation exists
        COMPACT,

        //both array-backed & hashset-backed representation exist.
        //this is needed as we are creating HashSet on contains()
        //but we don't want invalidate existing iterators.
        HYBRID,

        //only hashset based representation exists
        INFLATED
    }

    private final List<T> compactList;
    private Set<T> inflatedSet;
    private State state;

    public InflatableSet(int initialCapacity) {
        this.state = State.INITIAL_LOAD;
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Initial Capacity must be a positive number. Current capacity: "
                    + initialCapacity);
        }
        compactList = new ArrayList<T>(initialCapacity);
    }

    protected InflatableSet(InflatableSet other) {
        compactList = new ArrayList<T>(other.compactList.size());
        compactList.addAll(other.compactList);
        if (other.inflatedSet != null) {
            inflatedSet = new HashSet<T>(other.inflatedSet);
        }
        state = other.state;
    }


    @Override
    public int size() {
        if (state == State.INFLATED) {
            return inflatedSet.size();
        } else {
            return compactList.size();
        }
    }

    @Override
    public boolean isEmpty() {
        if (state == State.INFLATED) {
            return inflatedSet.isEmpty();
        } else {
            return compactList.isEmpty();
        }
    }

    @Override
    public boolean contains(Object o) {
        if (state == State.INITIAL_LOAD) {
            return compactList.contains(o);
        }
        if (state == State.COMPACT) {
            toHybridState();
        }
        return inflatedSet.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        if (state == State.INFLATED) {
            return inflatedSet.iterator();
        }
        return new HybridIterator();
    }

    @Override
    public boolean add(T t) {
        if (state == State.INITIAL_LOAD) {
            return compactList.add(t);
        }
        toInflatedState();
        return inflatedSet.add(t);
    }

    @Override
    public boolean remove(Object o) {
        switch (state) {
            case HYBRID:
                compactList.remove(o);
                return inflatedSet.remove(o);
            case INFLATED:
                return inflatedSet.remove(o);
            default:
                return compactList.remove(o);
        }
    }

    @Override
    public void clear() {
        switch (state) {
            case HYBRID:
                inflatedSet.clear();
                compactList.clear();
                break;
            case INFLATED:
                inflatedSet.clear();
                break;
            default:
                compactList.clear();
        }
    }

    public void close() {
        if (state != State.INITIAL_LOAD) {
            throw new IllegalStateException("InflatableSet can be only closed during InitialLoad. Current state: "
                    + state);
        }
        state = State.COMPACT;
    }

    /**
     * Returns a shallow copy of this <tt>InflatableSet</tt> instance: the keys and
     * values themselves are not cloned.
     *
     * @return a shallow copy of this set
     */
    @Override
    protected Object clone()  {
        return new InflatableSet<T>(this);
    }

    private void inflateIfNeeded() {
        if (inflatedSet == null) {
            inflatedSet = new HashSet<T>(compactList);
        }
    }

    private void toHybridState() {
        if (state == State.HYBRID) {
            return;
        }

        state = State.HYBRID;
        inflateIfNeeded();
    }

    private void toInflatedState() {
        if (state == State.INFLATED) {
            return;
        }

        state = State.INFLATED;
        inflateIfNeeded();
        invalidateIterators();
    }

    private void invalidateIterators() {
        if (compactList.size() == 0) {
            compactList.clear();
        } else {
            compactList.remove(0);
        }
    }

    private class HybridIterator implements Iterator<T> {
        private Iterator<T> innerIterator;
        private T currentValue;

        public HybridIterator() {
            innerIterator = compactList.iterator();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public T next() {
            currentValue = innerIterator.next();
            return currentValue;
        }

        @Override
        public void remove() {
            innerIterator.remove();
            if (inflatedSet != null) {
                inflatedSet.remove(currentValue);
            }
        }
    }
}
