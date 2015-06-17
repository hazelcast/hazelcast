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

package com.hazelcast.map.impl;

import com.hazelcast.nio.serialization.Data;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Specialization of ConcurrethHashMap for the near cache. Masks out the
 * <code>PendingValue</code> sentinel records from results.
 */
public class NearCacheConcurrentMap extends ConcurrentHashMap<Data, NearCacheRecord>
{
    @Override
    public NearCacheRecord get(Object key) {
        final NearCacheRecord rec = super.get(key);
        return rec.isPending()? null : rec;
    }

    @Override
    public Collection<NearCacheRecord> values() {
        return null;
    }

    @Override
    public Set<Entry<Data, NearCacheRecord>> entrySet() {
        return null;
    }

    @Override
    public NearCacheRecord putIfAbsent(Data key, NearCacheRecord value) {
        NearCacheRecord prev = super.putIfAbsent(key, value);
        while (prev != null && prev.isPending() && !super.replace(key, prev, value)) {
            prev = super.get(key);
        }
        return prev;
    }

    final class Values extends AbstractCollection<NearCacheRecord> {
        Values() {
        }

        public Iterator<NearCacheRecord> iterator() {
            return new ValueIterator();
        }

        public int size() {
            return NearCacheConcurrentMap.this.size();
        }

        public boolean contains(Object var1) {
            return NearCacheConcurrentMap.this.containsValue(var1);
        }

        public void clear() {
            NearCacheConcurrentMap.this.clear();
        }
    }

    final class ValueIterator implements Iterator<NearCacheRecord> {
        ValueIterator() {
            this.wrapped = null;
        }

        public NearCacheRecord next() {
            return super.nextEntry().value;
        }

        public V nextElement() {
            return super.nextEntry().value;
        }
    }


}
