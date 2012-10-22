///*
// * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.impl;
//
//import com.hazelcast.core.Instance;
//import com.hazelcast.core.MapEntry;
//import com.hazelcast.core.MultiMap;
//import com.hazelcast.core.Prefix;
//import com.hazelcast.impl.base.KeyValue;
//import com.hazelcast.impl.base.Pairs;
//import com.hazelcast.instance.HazelcastInstanceImpl;
//import com.hazelcast.instance.ThreadContext;
//import com.hazelcast.transaction.TransactionImpl;
//import com.hazelcast.nio.Data;
//import com.hazelcast.query.Predicate;
//import com.hazelcast.util.SimpleMapEntry;
//
//import java.util.*;
//
//import static com.hazelcast.impl.ClusterOperation.*;
//
//public class Entries extends AbstractSet {
//    private final Collection<Map.Entry> colKeyValues;
//    private final String name;
//    private final ClusterOperation operation;
//    private final boolean checkValue;
//    private final HazelcastInstanceImpl instance;
//
//    public Entries(HazelcastInstanceImpl instance, String name, ClusterOperation operation, Predicate predicate) {
//        this.instance = instance;
//        this.name = name;
//        this.operation = operation;
//        if (name.startsWith(Prefix.MULTIMAP)) {
//            colKeyValues = new LinkedList<Map.Entry>();
//        } else {
//            colKeyValues = new HashSet<Map.Entry>();
//        }
//        TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
//        this.checkValue = (Instance.InstanceType.MAP == Prefix.getInstanceType(name)) &&
//                (operation == CONCURRENT_MAP_ITERATE_VALUES
//                        || operation == CONCURRENT_MAP_ITERATE_ENTRIES);
//        if (txn != null) {
//            List<Map.Entry> entriesUnderTxn = txn.newEntries(name);
//            if (entriesUnderTxn != null) {
//                if (predicate != null) {
//                    for (Map.Entry entry : entriesUnderTxn) {
//                        if (predicate.apply((MapEntry) entry)) {
//                            colKeyValues.add(entry);
//                        }
//                    }
//                } else {
//                    colKeyValues.addAll(entriesUnderTxn);
//                }
//            }
//        }
//    }
//
//    public int size() {
//        return colKeyValues.size();
//    }
//
//    public Iterator iterator() {
//        return new EntryIterator(colKeyValues.iterator());
//    }
//
//    public void clearEntries() {
//        colKeyValues.clear();
//    }
//
//    public void addEntries(Pairs pairs) {
//        if (pairs == null) return;
//        if (pairs.getKeyValues() == null) return;
//        TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
//        for (KeyValue entry : pairs.getKeyValues()) {
//            if (txn != null) {
//                Object key = entry.getKey();
//                if (txn.has(name, key)) {
//                    Data value = txn.get(name, key);
//                    if (value != null) {
//                        colKeyValues.add(new SimpleMapEntry(instance, name, key, value));
//                    }
//                } else {
//                    entry.setName(instance, name);
//                    colKeyValues.add(entry);
//                }
//            } else {
//                entry.setName(instance, name);
//                colKeyValues.add(entry);
//            }
//        }
//    }
//
//    public Collection<Map.Entry> getKeyValues() {
//        return colKeyValues;
//    }
//
//    class EntryIterator implements Iterator {
//        final Iterator<Map.Entry> it;
//        Map.Entry entry = null;
//        boolean calledHasNext = false;
//        boolean calledNext = false;
//        boolean hasNext = false;
//
//        public EntryIterator(Iterator<Map.Entry> it) {
//            super();
//            this.it = it;
//        }
//
//        public boolean hasNext() {
//            if (calledHasNext && !calledNext) {
//                return hasNext;
//            }
//            calledNext = false;
//            calledHasNext = true;
//            hasNext = setHasNext();
//            return hasNext;
//        }
//
//        public boolean setHasNext() {
//            if (!it.hasNext()) {
//                return false;
//            }
//            entry = it.next();
//            if (checkValue && entry.getValue() == null) {
//                return hasNext();
//            }
//            return true;
//        }
//
//        public Object next() {
//            if (!calledHasNext) {
//                hasNext();
//            }
//            calledHasNext = false;
//            calledNext = true;
//            if (operation == CONCURRENT_MAP_ITERATE_KEYS
//                    || operation == CONCURRENT_MAP_ITERATE_KEYS_ALL) {
//                return entry.getKey();
//            } else if (operation == CONCURRENT_MAP_ITERATE_VALUES) {
//                return entry.getValue();
//            } else if (operation == CONCURRENT_MAP_ITERATE_ENTRIES) {
//                return entry;
//            } else throw new RuntimeException("Unknown iteration type " + operation);
//        }
//
//        public void remove() {
//            if (Prefix.getInstanceType(name) == Instance.InstanceType.MULTIMAP) {
//                if (operation == CONCURRENT_MAP_ITERATE_KEYS) {
//                    ((MultiMap) instance.getOrCreateInstance(name)).remove(entry.getKey(), null);
//                } else {
//                    ((MultiMap) instance.getOrCreateInstance(name)).remove(entry.getKey(), entry.getValue());
//                }
//            } else {
//                ((IRemoveAwareProxy) instance.getOrCreateInstance(name)).removeKey(entry.getKey());
//            }
//            it.remove();
//        }
//    }
//
//    public boolean add(Object o) {
//        throw new UnsupportedOperationException();
//    }
//
//    public boolean addAll(Collection c) {
//        throw new UnsupportedOperationException();
//    }
//
//    public void clear() {
//        throw new UnsupportedOperationException();
//    }
//
//    public boolean containsAll(Collection c) {
//        throw new UnsupportedOperationException();
//    }
//
//    public boolean remove(Object o) {
//        throw new UnsupportedOperationException();
//    }
//
//    public boolean removeAll(Collection c) {
//        throw new UnsupportedOperationException();
//    }
//
//    public boolean retainAll(Collection c) {
//        throw new UnsupportedOperationException();
//    }
//
//    public Object[] toArray() {
//        return toArray(null);
//    }
//
//    public Object[] toArray(Object[] a) {
//        List values = new ArrayList();
//        Iterator it = iterator();
//        while (it.hasNext()) {
//            Object obj = it.next();
//            if (obj != null) {
//                values.add(obj);
//            }
//        }
//        if (a == null) {
//            return values.toArray();
//        } else {
//            return values.toArray(a);
//        }
//    }
//}
