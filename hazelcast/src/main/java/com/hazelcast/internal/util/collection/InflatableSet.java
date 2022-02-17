/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import com.hazelcast.internal.serialization.SerializableByConvention;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Provides fast {@link Set} implementation for cases where items are known to not
 * contain duplicates.
 * <p>
 * It requires creation via {@link com.hazelcast.internal.util.collection.InflatableSet.Builder}
 * <p>
 * The builder doesn't call equals/hash methods on initial data insertion, hence it avoids
 * performance penalty in the case these methods are expensive. It also means it does
 * not detect duplicates - it's the responsibility of the caller to make sure no duplicated
 * entries are inserted.
 * <p>
 * Once InflatableSet is constructed via {@link Builder#build()} then it acts as a regular set. It has
 * been designed to mimic {@link HashSet}. On new entry insertion or lookup via
 * {@link #contains(Object)} it inflates itself: The backing list is copied into
 * internal {@link HashSet}. This obviously costs time and space. We are making a bet the
 * Set won't be modified in most cases.
 * <p>
 * It's intended to be used in cases where the contract mandates us to return Set,
 * but we know our data does not contain duplicates. It performs best in cases
 * biased towards sequential iteration.
 *
 * @param <T> the type of elements maintained by this set
 */
@SerializableByConvention
public class InflatableSet<T> extends AbstractSet<T> implements Set<T>, Serializable, Cloneable {

    private static final long serialVersionUID = 0L;

    enum State {
        // only array-backed representation exists
        COMPACT,

        // both array-backed & hashset-backed representation exist.
        // this is needed as we are creating HashSet on contains()
        // but we don't want invalidate existing iterators.
        HYBRID,

        // only hashset based representation exists
        INFLATED
    }

    /**
     * The field is package-visible to use in {@link com.hazelcast.internal.util.collection.ImmutableInflatableSet#iterator}.
     */
    State state;
    private Set<T> inflatedSet;
    private final List<T> compactList;


    /**
     * This constructor is intended to be used by {@link com.hazelcast.internal.util.collection.InflatableSet.Builder} and
     * {@link com.hazelcast.internal.util.collection.ImmutableInflatableSet.ImmutableSetBuilder} only.
     *
     * The constructor is package-visible to support {@link com.hazelcast.internal.util.collection.ImmutableInflatableSet}
     *
     * @param compactList list of elements for the InflatableSet
     */
    InflatableSet(List<T> compactList) {
        this.state = State.COMPACT;
        this.compactList = compactList;
    }

    /**
     * This copy-constructor is intended to be used by {@link #clone()} method only.
     *
     * @param other other InflatableSet which should be cloned
     */
    private InflatableSet(InflatableSet<T> other) {
        compactList = new ArrayList<T>(other.compactList.size());
        compactList.addAll(other.compactList);
        if (other.inflatedSet != null) {
            inflatedSet = new HashSet<T>(other.inflatedSet);
        }
        state = other.state;
    }

    public static <T> Builder<T> newBuilder(int initialCapacity) {
        return new Builder<T>(initialCapacity);
    }

    public static <T> Builder<T> newBuilder(List<T> list) {
        return new Builder<T>(list);
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

    /**
     * Returns a shallow copy of this <tt>InflatableSet</tt> instance: the keys and
     * values themselves are not cloned.
     *
     * @return a shallow copy of this set
     */
    @Override
    @SuppressFBWarnings(value = "CN_IDIOM", justification = "Deliberate, documented contract violation")
    @SuppressWarnings({"checkstyle:superclone", "CloneDoesntCallSuperClone"})
    protected Object clone() {
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

    class HybridIterator implements Iterator<T> {

        private Iterator<T> innerIterator;
        private T currentValue;

        HybridIterator() {
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

    abstract static class AbstractBuilder<T> {
        List<T> list;

        AbstractBuilder(int initialCapacity) {
            this.list = new ArrayList<>(initialCapacity);
        }

        AbstractBuilder(List<T> list) {
            this.list = checkNotNull(list, "list cannot be null");
        }

        public int size() {
            return list.size();
        }

        AbstractBuilder<T> add(T item) {
            list.add(item);
            return this;
        }
    }

    /**
     * Builder for {@link InflatableSet}.
     * This is the only way to create a new instance of InflatableSet.
     *
     * @param <T> the type of elements maintained by this set
     */
    public static final class Builder<T> extends AbstractBuilder<T> {

        private Builder(int initialCapacity) {
            super(initialCapacity);
        }

        private Builder(List<T> list) {
            super(list);
        }

        public Builder<T> add(T item) {
            super.add(item);
            return this;
        }

        public InflatableSet<T> build() {
            InflatableSet<T> set = new InflatableSet<>(list);

            // make sure no further insertions are possible
            list = Collections.emptyList();
            return set;
        }
    }
}
