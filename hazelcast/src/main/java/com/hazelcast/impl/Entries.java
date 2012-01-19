/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.core.Instance;
import com.hazelcast.core.MapEntry;
import com.hazelcast.core.MultiMap;
import com.hazelcast.impl.base.KeyValue;
import com.hazelcast.impl.base.Pairs;
import com.hazelcast.query.Predicate;

import java.util.*;

import static com.hazelcast.impl.ClusterOperation.*;

public class Entries extends AbstractSet {
    final Set<Map.Entry> lsKeyValues = new HashSet<Map.Entry>();
    final String name;
    final ClusterOperation operation;
    final boolean checkValue;
    final Predicate predicate;
    private ConcurrentMapManager concurrentMapManager;

    public Entries(ConcurrentMapManager concurrentMapManager, String name, ClusterOperation operation, Predicate predicate) {
        this.concurrentMapManager = concurrentMapManager;
        this.name = name;
        this.operation = operation;
        this.predicate = predicate;
        TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
        this.checkValue = (Instance.InstanceType.MAP == BaseManager.getInstanceType(name)) &&
                (operation == CONCURRENT_MAP_ITERATE_VALUES
                        || operation == CONCURRENT_MAP_ITERATE_ENTRIES);
        if (txn != null) {
            List<Map.Entry> entriesUnderTxn = txn.newEntries(name);
            if (entriesUnderTxn != null) {
                if (predicate != null) {
                    for (Map.Entry entry : entriesUnderTxn) {
                        if (predicate.apply((MapEntry) entry)) {
                            lsKeyValues.add(entry);
                        }
                    }
                } else {
                    lsKeyValues.addAll(entriesUnderTxn);
                }
            }
        }
    }

    public int size() {
        return lsKeyValues.size();
    }

    public Iterator iterator() {
        return new EntryIterator(lsKeyValues.iterator());
    }

    public void addEntries(Pairs pairs) {
        if (pairs == null) return;
        if (pairs.getKeyValues() == null) return;
        TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
        for (KeyValue entry : pairs.getKeyValues()) {
            if (txn != null) {
                Object key = entry.getKey();
                if (txn.has(name, key)) {
                    Object value = txn.get(name, key);
                    if (value != null) {
                        lsKeyValues.add(BaseManager.createSimpleMapEntry(concurrentMapManager.node.factory, name, key, value));
                    }
                } else {
                    entry.setName(concurrentMapManager.node.factory, name);
                    lsKeyValues.add(entry);
                }
            } else {
                entry.setName(concurrentMapManager.node.factory, name);
                lsKeyValues.add(entry);
            }
        }
    }

    public Set<Map.Entry> getKeyValues() {
        return lsKeyValues;
    }

    class EntryIterator implements Iterator {
        final Iterator<Map.Entry> it;
        Map.Entry entry = null;
        boolean calledHasNext = false;
        boolean calledNext = false;
        boolean hasNext = false;

        public EntryIterator(Iterator<Map.Entry> it) {
            super();
            this.it = it;
        }

        public boolean hasNext() {
            if (calledHasNext && !calledNext) {
                return hasNext;
            }
            calledNext = false;
            calledHasNext = true;
            hasNext = setHasNext();
            return hasNext;
        }

        public boolean setHasNext() {
            if (!it.hasNext()) {
                return false;
            }
            entry = it.next();
            if (checkValue && entry.getValue() == null) {
                return hasNext();
            }
            return true;
        }

        public Object next() {
            if (!calledHasNext) {
                hasNext();
            }
            calledHasNext = false;
            calledNext = true;
            if (operation == CONCURRENT_MAP_ITERATE_KEYS
                    || operation == CONCURRENT_MAP_ITERATE_KEYS_ALL) {
                return entry.getKey();
            } else if (operation == CONCURRENT_MAP_ITERATE_VALUES) {
                return entry.getValue();
            } else if (operation == CONCURRENT_MAP_ITERATE_ENTRIES) {
                return entry;
            } else throw new RuntimeException("Unknown iteration type " + operation);
        }

        public void remove() {
            if (BaseManager.getInstanceType(name) == Instance.InstanceType.MULTIMAP) {
                if (operation == CONCURRENT_MAP_ITERATE_KEYS) {
                    ((MultiMap) concurrentMapManager.node.factory.getOrCreateProxyByName(name)).remove(entry.getKey(), null);
                } else {
                    ((MultiMap) concurrentMapManager.node.factory.getOrCreateProxyByName(name)).remove(entry.getKey(), entry.getValue());
                }
            } else {
                ((IRemoveAwareProxy) concurrentMapManager.node.factory.getOrCreateProxyByName(name)).removeKey(entry.getKey());
            }
            it.remove();
        }
    }

    public boolean add(Object o) {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public Object[] toArray() {
        return toArray(null);
    }

    public Object[] toArray(Object[] a) {
        List values = new ArrayList();
        Iterator it = iterator();
        while (it.hasNext()) {
            Object obj = it.next();
            if (obj != null) {
                values.add(obj);
            }
        }
        if (a == null) {
            return values.toArray();
        } else {
            return values.toArray(a);
        }
    }
}
