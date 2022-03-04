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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.MapUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.hazelcast.query.impl.AbstractIndex.NULL;

/**
 * Base class for concrete index store implementations
 */
public abstract class BaseIndexStore implements IndexStore {

    static final float LOAD_FACTOR = 0.75F;

    private final ReentrantReadWriteLock lock;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;

    private final CopyFunctor<Data, QueryableEntry> resultCopyFunctor;

    BaseIndexStore(IndexCopyBehavior copyOn, boolean enableGlobalLock) {
        if (copyOn == IndexCopyBehavior.COPY_ON_WRITE || copyOn == IndexCopyBehavior.NEVER) {
            resultCopyFunctor = new PassThroughFunctor();
        } else {
            resultCopyFunctor = new CopyInputFunctor();
        }
        lock = enableGlobalLock ? new ReentrantReadWriteLock() : null;
        readLock = enableGlobalLock ? lock.readLock() : null;
        writeLock = enableGlobalLock ? lock.writeLock() : null;
    }

    /**
     * Canonicalizes the given value for storing it in this index store.
     * <p>
     * The method is used by hash indexes to achieve the canonical
     * representation of mixed-type numeric values, so {@code equals} and {@code
     * hashCode} logic can work properly.
     * <p>
     * The main difference comparing to {@link IndexStore#canonicalizeQueryArgumentScalar}
     * is that this method is specifically designed to support the
     * canonicalization of persistent index values (think of map entry attribute
     * values), so a more suitable value representation may chosen.
     *
     * @param value the value to canonicalize.
     * @return the canonicalized value.
     */
    abstract Comparable canonicalizeScalarForStorage(Comparable value);

    void takeWriteLock() {
        if (lock != null) {
            writeLock.lock();
        }
    }

    void releaseWriteLock() {
        if (lock != null) {
            writeLock.unlock();
        }
    }

    void takeReadLock() {
        if (lock != null) {
            readLock.lock();
        }
    }

    void releaseReadLock() {
        if (lock != null) {
            readLock.unlock();
        }
    }

    final void copyToMultiResultSet(MultiResultSet resultSet, Map<Data, QueryableEntry> records) {
        resultSet.addResultSet(resultCopyFunctor.invoke(records));
    }

    final Set<QueryableEntry> toSingleResultSet(Map<Data, QueryableEntry> records) {
        return new SingleResultSet(resultCopyFunctor.invoke(records));
    }

    @Override
    public void destroy() {
        // nothing to destroy
    }

    Comparable sanitizeValue(Object input) {
        if (input instanceof CompositeValue) {
            CompositeValue compositeValue = (CompositeValue) input;
            Comparable[] components = compositeValue.getComponents();
            for (int i = 0; i < components.length; ++i) {
                components[i] = sanitizeScalar(components[i]);
            }
            return compositeValue;
        } else {
            return sanitizeScalar(input);
        }
    }

    private Comparable sanitizeScalar(Object input) {
        if (input == null || input instanceof Comparable) {
            Comparable value = (Comparable) input;
            if (value == null) {
                value = NULL;
            } else if (value.getClass().isEnum()) {
                value = TypeConverters.ENUM_CONVERTER.convert(value);
            }
            return canonicalizeScalarForStorage(value);
        } else {
            throw new IllegalArgumentException("It is not allowed to use a type that is not Comparable: " + input.getClass());
        }
    }

    interface CopyFunctor<A, B> {
        Map<A, B> invoke(Map<A, B> map);
    }

    interface IndexFunctor<A, B> {
        Object invoke(A param1, B param2);
    }

    private static class PassThroughFunctor implements CopyFunctor<Data, QueryableEntry> {

        @Override
        public Map<Data, QueryableEntry> invoke(Map<Data, QueryableEntry> map) {
            return map;
        }
    }

    private static class CopyInputFunctor implements CopyFunctor<Data, QueryableEntry> {

        @Override
        public Map<Data, QueryableEntry> invoke(Map<Data, QueryableEntry> map) {
            return MapUtil.isNullOrEmpty(map) ? map : new HashMap<>(map);
        }
    }
}
