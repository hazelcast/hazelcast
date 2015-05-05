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

import java.util.Iterator;
import java.util.Map;

class ValuesIteratorFactory<K, V>
        implements IteratorFactory<K, V, V> {

    private final ReplicatedRecordStore recordStore;

    ValuesIteratorFactory(ReplicatedRecordStore recordStore) {
        this.recordStore = recordStore;
    }

    @Override
    public Iterator<V> create(final Iterator<Map.Entry<K, ReplicatedRecord<K, V>>> iterator) {
        return new Iterator<V>() {
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
            public V next() {
                Object value = recordStore.unmarshallValue(entry.getValue().getValue());
                return (V) value;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Lazy structures are not modifiable");
            }
        };
    }
}
