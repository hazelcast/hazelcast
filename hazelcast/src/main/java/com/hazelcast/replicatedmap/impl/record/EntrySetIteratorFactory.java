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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.replicatedmap.impl.record.LazySet.IteratorFactory;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

class EntrySetIteratorFactory<K, V>
        implements IteratorFactory<K, V, Map.Entry<K, V>> {

    private final ReplicatedRecordStore recordStore;

    EntrySetIteratorFactory(ReplicatedRecordStore recordStore) {
        this.recordStore = recordStore;
    }

    @Override
    public Iterator<Map.Entry<K, V>> create(final Iterator<Map.Entry<K, ReplicatedRecord<K, V>>> iterator) {
        return new Iterator<Map.Entry<K, V>>() {
            private Map.Entry<K, ReplicatedRecord<K, V>> entry;

            @Override
            public boolean hasNext() {
                while (iterator.hasNext()) {
                    entry = iterator.next();
                    if (!entry.getValue().isTombstone()) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Map.Entry<K, V> next() {
                Object key = recordStore.unmarshallKey(entry.getKey());
                Object value = recordStore.unmarshallValue(entry.getValue().getValue());
                return new AbstractMap.SimpleEntry<K, V>((K) key, (V) value);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Lazy structures are not modifiable");
            }
        };
    }
}
