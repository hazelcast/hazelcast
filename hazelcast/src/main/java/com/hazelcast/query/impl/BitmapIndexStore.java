/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.monitor.impl.IndexOperationStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.getters.MultiResult;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.InPredicate;
import com.hazelcast.query.impl.predicates.NotEqualPredicate;
import com.hazelcast.query.impl.predicates.OrPredicate;
import com.hazelcast.util.collection.Long2LongHashMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class BitmapIndexStore extends BaseIndexStore {

    private static final Object CONSUMED = new Object();

    private static final Set<Class<? extends Predicate>> EVALUABLE_PREDICATES = new HashSet<Class<? extends Predicate>>();

    static {
        EVALUABLE_PREDICATES.add(EqualPredicate.class);
        EVALUABLE_PREDICATES.add(NotEqualPredicate.class);
        EVALUABLE_PREDICATES.add(InPredicate.class);
        EVALUABLE_PREDICATES.add(OrPredicate.class);
    }

    // TODO support index stats

    private final String keyAttribute;
    private final InternalSerializationService serializationService;
    private final Extractors extractors;

    private final Bitmaps<QueryableEntry> bitmaps = new Bitmaps<QueryableEntry>(LOAD_FACTOR);
    private final Long2LongHashMap internalKeys;
    private long internalKeyCounter = 0;

    public BitmapIndexStore(String keyAttribute, InternalSerializationService serializationService, Extractors extractors) {
        super(IndexCopyBehavior.NEVER);
        if (keyAttribute.endsWith("?")) {
            this.keyAttribute = keyAttribute.substring(0, keyAttribute.length() - 1);
            // TODO enforce non-negative keys only (?), see com.hazelcast.util.collection.Long2LongHashMap.missingValue
            this.internalKeys = new Long2LongHashMap(Long2LongHashMap.DEFAULT_INITIAL_CAPACITY, 0.75, -1);
        } else {
            this.keyAttribute = keyAttribute;
            this.internalKeys = null;
        }
        this.serializationService = serializationService;
        this.extractors = extractors;
    }

    @Override
    public Comparable canonicalizeQueryArgumentScalar(Comparable value) {
        // Using a storage representation for arguments here to save on
        // conversions later.
        return canonicalizeScalarForStorage(value);
    }

    @Override
    public void insert(Object value, QueryableEntry entry, IndexOperationStats operationStats) {
        long key = extractKey(entry);
        Iterator values = makeIterator(value);

        takeWriteLock();
        try {
            if (internalKeys != null) {
                long internalKey = internalKeyCounter++;
                long replaced = internalKeys.put(key, internalKey);
                assert replaced == -1;
                key = internalKey;
            }
            bitmaps.insert(values, key, entry);
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void update(Object oldValue, Object newValue, QueryableEntry entry, IndexOperationStats operationStats) {
        long key = extractKey(entry);
        Iterator oldValues = makeIterator(oldValue);
        Iterator newValues = makeIterator(newValue);

        takeWriteLock();
        try {
            if (internalKeys != null) {
                key = internalKeys.get(key);
                assert key != -1;
            }
            bitmaps.update(oldValues, newValues, key, entry);
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void remove(Object value, Data entryKey, Object entryValue, IndexOperationStats operationStats) {
        long key = extractKey(entryKey, entryValue);
        Iterator values = makeIterator(value);

        takeWriteLock();
        try {
            if (internalKeys != null) {
                key = internalKeys.remove(key);
                assert key != -1;
            }
            bitmaps.remove(values, key);
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void clear() {
        takeWriteLock();
        try {
            bitmaps.clear();
            if (internalKeys != null) {
                internalKeys.clear();
                internalKeyCounter = 0;
            }
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public boolean canEvaluate(Class<? extends Predicate> predicateClass) {
        return EVALUABLE_PREDICATES.contains(predicateClass);
    }

    @Override
    public Set<QueryableEntry> evaluate(Predicate predicate, TypeConverter converter) {
        takeReadLock();
        try {
            return toSingleResultSet(toMap(bitmaps.evaluate(predicate, new CanonicalizingConverter(converter))));
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        takeReadLock();
        try {
            return toSingleResultSet(toMap(bitmaps.getHaving(canonicalize(value))));
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        takeReadLock();
        try {
            return toSingleResultSet(toMap(bitmaps.getHavingAnyOf(values)));
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        // TODO
        throw new UnsupportedOperationException();
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
        Map<Data, QueryableEntry> map = new HashMap<Data, QueryableEntry>();
        while (iterator.hasNext()) {
            QueryableEntry entry = iterator.next();
            map.put(entry.getKeyData(), entry);
        }
        return map;
    }

    private long extractKey(Data entryKey, Object entryValue) {
        Object key =
                QueryableEntry.extractAttributeValue(extractors, serializationService, keyAttribute, entryKey, entryValue, null);
        if (key == null) {
            throw new NullPointerException("non-null unique key value is required");
        }
        if (!Numbers.isLongRepresentable(key.getClass())) {
            throw new NullPointerException("integer-value unique key value is required");
        }
        return ((Number) key).longValue();
    }

    private long extractKey(QueryableEntry entry) {
        Object key = entry.getAttributeValue(keyAttribute);
        if (key == null) {
            throw new NullPointerException("non-null unique key value is required");
        }
        if (!Numbers.isLongRepresentable(key.getClass())) {
            throw new NullPointerException("integer-value unique key value is required");
        }
        return ((Number) key).longValue();
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

    private class MultiValueIterator implements Iterator {

        private final Iterator iterator;

        public MultiValueIterator(MultiResult multiResult) {
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

    private class SingleValueIterator implements Iterator {

        private Object value;

        public SingleValueIterator(Object value) {
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

    private class CanonicalizingConverter implements TypeConverter {

        private final TypeConverter converter;

        public CanonicalizingConverter(TypeConverter converter) {
            this.converter = converter;
        }

        @Override
        public Comparable convert(Comparable value) {
            return canonicalize(converter.convert(value));
        }

    }

}
