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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.replicatedmap.impl.record.LazySet.IteratorFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

class ValuesIteratorFactory<K, V> implements IteratorFactory<K, V, V> {

    private final ReplicatedRecordStore recordStore;

    ValuesIteratorFactory(ReplicatedRecordStore recordStore) {
        this.recordStore = recordStore;
    }

    @Override
    public Iterator<V> create(Iterator<Map.Entry<K, ReplicatedRecord<K, V>>> iterator) {
        return new ValuesIterator(iterator);
    }

    private final class ValuesIterator implements Iterator<V> {

        private final Iterator<Map.Entry<K, ReplicatedRecord<K, V>>> iterator;

        private Map.Entry<K, ReplicatedRecord<K, V>> entry;

        ValuesIterator(Iterator<Map.Entry<K, ReplicatedRecord<K, V>>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            while (iterator.hasNext()) {
                entry = iterator.next();
                if (testEntry(entry)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public V next() {
            Map.Entry<K, ReplicatedRecord<K, V>> entry = this.entry;
            Object value = entry != null && entry.getValue() != null ? entry.getValue().getValue() : null;

            while (entry == null) {
                entry = findNextEntry();

                ReplicatedRecord<K, V> record = entry.getValue();
                value = record != null ? record.getValue() : null;

                if (value != null) {
                    break;
                }
            }

            this.entry = null;
            if (value == null) {
                throw new NoSuchElementException();
            }

            value = recordStore.unmarshall(value);
            return (V) value;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Lazy structures are not modifiable");
        }

        private boolean testEntry(Map.Entry<K, ReplicatedRecord<K, V>> entry) {
            return entry.getKey() != null && entry.getValue() != null;
        }

        private Map.Entry<K, ReplicatedRecord<K, V>> findNextEntry() {
            Map.Entry<K, ReplicatedRecord<K, V>> entry = null;
            while (iterator.hasNext()) {
                entry = iterator.next();
                if (testEntry(entry)) {
                    break;
                }
                entry = null;
            }
            if (entry == null) {
                throw new NoSuchElementException();
            }
            return entry;
        }
    }
}
