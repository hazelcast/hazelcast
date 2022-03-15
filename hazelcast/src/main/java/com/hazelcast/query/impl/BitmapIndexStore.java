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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.json.NonTerminalJsonValue;
import com.hazelcast.internal.monitor.impl.IndexOperationStats;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.collection.Long2LongHashMap;
import com.hazelcast.internal.util.collection.Object2LongHashMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.bitmap.Bitmap;
import com.hazelcast.query.impl.getters.MultiResult;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.InPredicate;
import com.hazelcast.query.impl.predicates.NotEqualPredicate;
import com.hazelcast.query.impl.predicates.NotPredicate;
import com.hazelcast.query.impl.predicates.OrPredicate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * The store of bitmap indexes.
 * <p>
 * Internally, manages a {@link Bitmap} instance along with key remapping
 * structures used to establish the correspondence between long bitmap keys and
 * actual user-provided keys.
 */
@SuppressWarnings({"rawtypes", "checkstyle:MethodCount"})
public final class BitmapIndexStore extends BaseIndexStore {

    private static final long NO_KEY = -1;
    private static final int INITIAL_CAPACITY = 8;
    private static final float LOAD_FACTOR = 0.75F;

    private static final Object CONSUMED = new Object();

    private static final Set<Class<? extends Predicate>> EVALUABLE_PREDICATES = new HashSet<>();

    static {
        EVALUABLE_PREDICATES.add(AndPredicate.class);
        EVALUABLE_PREDICATES.add(OrPredicate.class);
        EVALUABLE_PREDICATES.add(NotPredicate.class);

        EVALUABLE_PREDICATES.add(EqualPredicate.class);
        EVALUABLE_PREDICATES.add(NotEqualPredicate.class);
        EVALUABLE_PREDICATES.add(InPredicate.class);
    }

    private final String keyAttribute;

    private final Bitmap<QueryableEntry> bitmap = new Bitmap<>();
    // maps user-provided long keys to long bitmap keys
    private final Long2LongHashMap internalKeys;
    // maps user-provided object keys to long bitmap keys
    private final Object2LongHashMap internalObjectKeys;
    private long internalKeyCounter;

    public BitmapIndexStore(IndexConfig config) {
        super(IndexCopyBehavior.NEVER, true);

        this.keyAttribute = config.getBitmapIndexOptions().getUniqueKey();

        switch (config.getBitmapIndexOptions().getUniqueKeyTransformation()) {
            case OBJECT:
                // object-to-long remapping
                this.internalObjectKeys = new Object2LongHashMap(INITIAL_CAPACITY, LOAD_FACTOR, NO_KEY);
                this.internalKeys = null;
                break;
            case LONG:
                // long-to-long remapping
                this.internalKeys = new Long2LongHashMap(INITIAL_CAPACITY, LOAD_FACTOR, NO_KEY);
                this.internalObjectKeys = null;
                break;
            case RAW:
                // no remapping, raw attribute values are used as long keys
                this.internalKeys = null;
                this.internalObjectKeys = null;
                break;
            default:
                throw new IllegalArgumentException(
                        "unexpected unique key transform: " + config.getBitmapIndexOptions().getUniqueKeyTransformation());
        }
    }

    @Override
    public Comparable canonicalizeQueryArgumentScalar(Comparable value) {
        // Using a storage representation for arguments here to save on
        // conversions later.
        return canonicalizeScalarForStorage(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void insert(Object value, CachedQueryEntry entry, QueryableEntry entryToStore, IndexOperationStats operationStats) {
        if (value == NonTerminalJsonValue.INSTANCE) {
            return;
        }

        if (internalObjectKeys == null) {
            // no remapping or long-to-long remapping

            long key = extractLongKey(entry);
            Iterator values = makeIterator(value);

            takeWriteLock();
            try {
                if (internalKeys != null) {
                    // long-to-long remapping

                    long internalKey = internalKeyCounter++;
                    long replaced = internalKeys.put(key, internalKey);
                    assert replaced == NO_KEY;
                    key = internalKey;
                } else if (key < 0) {
                    throw makeNegativeKeyException(key);
                }

                bitmap.insert(values, key, entryToStore, operationStats);
            } finally {
                releaseWriteLock();
            }
        } else {
            // object-to-long remapping

            Object key = extractObjectKey(entry);
            Iterator values = makeIterator(value);

            takeWriteLock();
            try {
                long internalKey = internalKeyCounter++;
                long replaced = internalObjectKeys.put(key, internalKey);
                assert replaced == NO_KEY;
                bitmap.insert(values, internalKey, entryToStore, operationStats);
            } finally {
                releaseWriteLock();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void update(Object oldValue, Object newValue, CachedQueryEntry entry, QueryableEntry entryToStore,
                       IndexOperationStats operationStats) {
        if (oldValue == NonTerminalJsonValue.INSTANCE) {
            insert(newValue, entry, entryToStore, operationStats);
            return;
        }

        if (internalObjectKeys == null) {
            // no remapping or long-to-long remapping

            long key = extractLongKey(entry);
            Iterator oldValues = makeIterator(oldValue);
            Iterator newValues = makeIterator(newValue);

            takeWriteLock();
            try {
                if (internalKeys != null) {
                    // long-to-long remapping

                    long internalKey = internalKeys.get(key);
                    if (internalKey == NO_KEY) {
                        // see https://github.com/hazelcast/hazelcast/issues/17342#issuecomment-680840612
                        internalKey = internalKeyCounter++;
                        internalKeys.put(key, internalKey);
                        bitmap.insert(newValues, internalKey, entryToStore, operationStats);
                        return;
                    } else {
                        key = internalKey;
                    }
                } else if (key < 0) {
                    throw makeNegativeKeyException(key);
                }

                bitmap.update(oldValues, newValues, key, entryToStore, operationStats);
            } finally {
                releaseWriteLock();
            }
        } else {
            // object-to-long remapping

            Object key = extractObjectKey(entry);
            Iterator oldValues = makeIterator(oldValue);
            Iterator newValues = makeIterator(newValue);

            takeWriteLock();
            try {
                long internalKey = internalObjectKeys.getValue(key);
                if (internalKey == NO_KEY) {
                    // see https://github.com/hazelcast/hazelcast/issues/17342#issuecomment-680840612
                    internalKey = internalKeyCounter++;
                    internalObjectKeys.put(key, internalKey);
                    bitmap.insert(newValues, internalKey, entryToStore, operationStats);
                } else {
                    bitmap.update(oldValues, newValues, internalKey, entryToStore, operationStats);
                }
            } finally {
                releaseWriteLock();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void remove(Object value, CachedQueryEntry entry, IndexOperationStats operationStats) {
        if (value == NonTerminalJsonValue.INSTANCE) {
            return;
        }

        if (internalObjectKeys == null) {
            // no remapping or long-to-long remapping

            long key = extractLongKey(entry);
            Iterator values = makeIterator(value);

            takeWriteLock();
            try {
                if (internalKeys != null) {
                    // long-to-long remapping

                    key = internalKeys.remove(key);
                    if (key != NO_KEY) {
                        // see https://github.com/hazelcast/hazelcast/issues/15439 and
                        // https://github.com/hazelcast/hazelcast/issues/17342#issuecomment-680840612
                        bitmap.remove(values, key, operationStats);
                    }
                } else {
                    if (key < 0) {
                        throw makeNegativeKeyException(key);
                    }
                    bitmap.remove(values, key, operationStats);
                }
            } finally {
                releaseWriteLock();
            }
        } else {
            // object-to-long remapping

            Object key = extractObjectKey(entry);
            Iterator values = makeIterator(value);

            takeWriteLock();
            try {
                long internalKey = internalObjectKeys.removeKey(key);
                if (internalKey != NO_KEY) {
                    // see https://github.com/hazelcast/hazelcast/issues/15439 and
                    // https://github.com/hazelcast/hazelcast/issues/17342#issuecomment-680840612
                    bitmap.remove(values, internalKey, operationStats);
                }
            } finally {
                releaseWriteLock();
            }
        }
    }

    @Override
    public void clear() {
        takeWriteLock();
        try {
            bitmap.clear();
            if (internalKeys != null) {
                internalKeys.clear();
            }
            if (internalObjectKeys != null) {
                internalObjectKeys.clear();
            }
            internalKeyCounter = 0;
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public boolean isEvaluateOnly() {
        return true;
    }

    @Override
    public boolean canEvaluate(Class<? extends Predicate> predicateClass) {
        return EVALUABLE_PREDICATES.contains(predicateClass);
    }

    @Override
    public Set<QueryableEntry> evaluate(Predicate predicate, TypeConverter converter) {
        takeReadLock();
        try {
            return toSingleResultSet(toMap(bitmap.evaluate(predicate, new CanonicalizingConverter(converter))));
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(boolean descending) {
        throw makeUnsupportedOperationException();
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(Comparable value) {
        throw makeUnsupportedOperationException();
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(Comparison comparison, Comparable value, boolean descending) {
        throw makeUnsupportedOperationException();
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(
            Comparable from,
            boolean fromInclusive,
            Comparable to,
            boolean toInclusive,
            boolean descending
    ) {
        throw makeUnsupportedOperationException();
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(Comparable value) {
        throw makeUnsupportedOperationException();
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(boolean descending) {
        throw makeUnsupportedOperationException();
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(Comparison comparison, Comparable value, boolean descending) {
        throw makeUnsupportedOperationException();
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(
            Comparable from,
            boolean fromInclusive,
            Comparable to,
            boolean toInclusive,
            boolean descending
    ) {
        throw makeUnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        throw makeUnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        throw makeUnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        throw makeUnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        throw makeUnsupportedOperationException();
    }

    @Override
    Comparable canonicalizeScalarForStorage(Comparable value) {
        // Assuming on-heap overhead of 12 bytes for the object header and
        // allocation granularity by modulo 8, there is no point in trying to
        // represent a value in less than 4 bytes.

        if (!(value instanceof Number)) {
            return value;
        }

        Class clazz = value.getClass();
        Number number = (Number) value;

        if (clazz == Double.class) {
            double doubleValue = number.doubleValue();

            long longValue = number.longValue();
            if (Numbers.equalDoubles(doubleValue, (double) longValue)) {
                return canonicalizeLongRepresentable(longValue);
            }

            float floatValue = number.floatValue();
            if (doubleValue == (double) floatValue) {
                return floatValue;
            }
        } else if (clazz == Float.class) {
            float floatValue = number.floatValue();

            long longValue = number.longValue();
            if (Numbers.equalFloats(floatValue, (float) longValue)) {
                return canonicalizeLongRepresentable(longValue);
            }
        } else if (Numbers.isLongRepresentable(clazz)) {
            return canonicalizeLongRepresentable(number.longValue());
        }

        return value;
    }

    private Map<Data, QueryableEntry> toMap(Iterator<QueryableEntry> iterator) {
        Map<Data, QueryableEntry> map = new HashMap<>();
        while (iterator.hasNext()) {
            QueryableEntry entry = iterator.next();
            map.put(entry.getKeyData(), entry);
        }
        return map;
    }

    private long extractLongKey(QueryableEntry entry) {
        Object key = entry.getAttributeValue(keyAttribute);
        return extractLongKey(key);
    }

    private Object extractObjectKey(QueryableEntry entry) {
        Object key = entry.getAttributeValue(keyAttribute);
        return extractObjectKey(key);
    }

    private Iterator makeIterator(Object value) {
        return value instanceof MultiResult ? new MultiValueIterator((MultiResult) value) : new SingleValueIterator(value);
    }

    private static Comparable canonicalizeLongRepresentable(long value) {
        if (value == (long) (int) value) {
            return (int) value;
        } else {
            return value;
        }
    }

    private Comparable canonicalize(Comparable value) {
        return canonicalizeScalarForStorage(value);
    }

    private static UnsupportedOperationException makeUnsupportedOperationException() {
        return new UnsupportedOperationException("bitmap indexes support only direct predicate evaluation");
    }

    private static long extractLongKey(Object key) {
        if (key == null) {
            throw new NullPointerException("non-null unique key value is required");
        }
        if (!Numbers.isLongRepresentable(key.getClass())) {
            throw new IllegalArgumentException("integer-valued unique key value is required");
        }
        return ((Number) key).longValue();
    }

    private static Object extractObjectKey(Object key) {
        if (key == null) {
            throw new NullPointerException("non-null unique key value is required");
        }
        return key;
    }

    private IllegalArgumentException makeNegativeKeyException(long key) {
        return new IllegalArgumentException("negative keys are not supported: " + keyAttribute + " = " + key);
    }

    private final class MultiValueIterator implements Iterator {

        private final Iterator iterator;

        MultiValueIterator(MultiResult multiResult) {
            this.iterator = multiResult.getResults().iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Object next() {
            return sanitizeValue(iterator.next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private final class SingleValueIterator implements Iterator {

        private Object value;

        SingleValueIterator(Object value) {
            this.value = value;
        }

        @Override
        public boolean hasNext() {
            return value != CONSUMED;
        }

        @Override
        public Object next() {
            Comparable value = sanitizeValue(this.value);
            this.value = CONSUMED;
            return value;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * Converts and at the same time canonicalizes the values passed in.
     */
    private final class CanonicalizingConverter implements TypeConverter {

        private final TypeConverter converter;

        CanonicalizingConverter(TypeConverter converter) {
            this.converter = converter;
        }

        @Override
        public Comparable convert(Comparable value) {
            return canonicalize(converter.convert(value));
        }

    }
}
