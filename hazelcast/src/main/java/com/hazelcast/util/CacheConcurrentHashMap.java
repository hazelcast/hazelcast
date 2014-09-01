/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;


import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * ConcurrentHashMap to extend iterator capability
 *
 * @param <K>
 * @param <V>
 */
public class CacheConcurrentHashMap<K, V> extends ConcurrentReferenceHashMap<K, V> {


    private static final float LOAD_FACTOR = 0.91f;

    public CacheConcurrentHashMap(int initialCapacity) {
        this(initialCapacity, LOAD_FACTOR, 1, ReferenceType.STRONG, ReferenceType.STRONG, null);
    }

    public CacheConcurrentHashMap(int initialCapacity, float loadFactor, int concurrencyLevel,
                                  ReferenceType keyType, ReferenceType valueType, EnumSet<Option> options) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
    }

//    public CacheKeyIteratorResult keySet(int nextTableIndex, int size) {
//        KeyIteratorInitializable keyIterator = new KeyIteratorInitializable(nextTableIndex);
//        List<Data> keys = new ArrayList<Data>();
//        int counter = 0;
//        while ((counter < size || keyIterator.hasNextLinked()) && keyIterator.hasNext() ) {
//            final Data next = (Data) keyIterator.next();
//            keys.add(next);
//            counter++;
//        }
//        return new CacheKeyIteratorResult(keys, keyIterator.nextTableIndex);
//    }

    public CacheKeyIteratorResult fetchNext(int nextTableIndex, int size) {
        List<Data> keys = new ArrayList<Data>();
        int tableIndex = fetch(nextTableIndex, size, keys);

        return new CacheKeyIteratorResult(keys, tableIndex);
    }

    int fetch(int tableIndex, int size, List<Data> keys) {
        final long now = Clock.currentTimeMillis();
//        List<K> keys = new ArrayList<K>();
        int nextTableIndex;
        final Segment<K, V> segment = segments[0];
        HashEntry<K, V>[] currentTable = segment.table;
        if (tableIndex >= 0 && tableIndex < segment.table.length) {
            nextTableIndex = tableIndex;
        } else {
            nextTableIndex = currentTable.length - 1;
        }

        int counter = 0;
        while (nextTableIndex >= 0 && counter < size) {
            HashEntry<K, V> nextEntry = currentTable[nextTableIndex--];
            while (nextEntry != null) {
                if (nextEntry.key() != null) {
                    CacheRecord record = (CacheRecord) nextEntry.value();
                    boolean isExpired = record != null && record.isExpiredAt(now);
                    if (!isExpired) {
                        keys.add((Data) nextEntry.key());
                        counter++;
                    }
                }
                nextEntry = nextEntry.next;
            }
        }
        return nextTableIndex;
    }


////    public Iterator<K> iterator(int nextSegmentIndex, int nextTableIndex) {
////        KeyIteratorInitializable keyIterator = new KeyIteratorInitializable(nextTableIndex);
////        return keyIterator;
////    }
//
//    class KeyIteratorInitializable extends InitializableIterator implements Iterator<K> {
//
//        private boolean state = false;
//
//        public KeyIteratorInitializable(int tableIndex) {
//
//            state = init(tableIndex);
////            if(state){
////                advance();
////            }
//        }
//
//        public boolean hasNextLinked() {
//            return state && nextEntry != null && nextEntry.next != null;
//        }
//
//        @Override
//        public boolean hasNext() {
//            return state && super.hasNext();
//        }
//
//        public K next() {
//            if (state) {
//                return super.nextEntry().key();
//            }
//            return null;
//        }
//
//        public K nextElement() {
//            return next();
//        }
//    }
//
//    class InitializableIterator extends HashIterator{
//
//        boolean init(int tableIndex) {
//            nextSegmentIndex = segments.length - 1;
//            if (nextSegmentIndex < 0) {
//                return false;
//            }
//            final Segment<K, V> segment = segments[nextSegmentIndex--];
//            this.currentTable = segment.table;
//            if (tableIndex >= 0 && tableIndex < segment.table.length) {
//                nextTableIndex = tableIndex;
//            } else {
//                nextTableIndex = currentTable.length - 1;
//            }
//
//            while (nextTableIndex >= 0) {
//                if ((nextEntry = currentTable[nextTableIndex--]) != null) {
//                    return true;
//                }
//            }
////            while (nextSegmentIndex >= 0 && nextSegmentIndex < segments.length) {
////                Segment<K, V> seg = segments[nextSegmentIndex--];
////                if (seg.count != 0) {
////                    this.currentTable = seg.table;
////                    for (int j = currentTable.length - 1; j >= 0; --j) {
////                        if ((nextEntry = currentTable[j]) != null) {
////                            nextTableIndex = j - 1;
////                            return true;
////                        }
////                    }
////                }
////            }
//            return false;
//        }
//    }


}
