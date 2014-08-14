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


import com.hazelcast.cache.CacheKeyIteratorResult;
import com.hazelcast.nio.serialization.Data;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;

/**
 * ConcurrentHashMap to extend iterator capability
 */
public class CacheConcurrentHashMap<K,V> extends ConcurrentReferenceHashMap<K,V> {


    public CacheConcurrentHashMap(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL,
                ReferenceType.STRONG, ReferenceType.STRONG, null);
    }

    public CacheConcurrentHashMap(int initialCapacity, float loadFactor, int concurrencyLevel,
                                      ReferenceType keyType, ReferenceType valueType, EnumSet<Option> options) {
        super(initialCapacity,loadFactor,concurrencyLevel,keyType,valueType,options);
    }

    public CacheKeyIteratorResult keySet(int nextSegmentIndex, int nextTableIndex,int size){
        KeyIteratorInitializable keyIterator = new KeyIteratorInitializable(nextSegmentIndex,nextTableIndex);
        HashSet<Data> keys = new HashSet<Data>();
        int counter=0;
        while (keyIterator.hasNext() && counter < size){
            keys.add((Data) keyIterator.next());
            counter++;
        }
        return new CacheKeyIteratorResult(keys, keyIterator.nextSegmentIndex, keyIterator.nextTableIndex);
    }

    public Iterator<K> iterator(int nextSegmentIndex, int nextTableIndex){
        KeyIteratorInitializable keyIterator = new KeyIteratorInitializable(nextSegmentIndex,nextTableIndex);
        return keyIterator;
    }

    class KeyIteratorInitializable extends InitializableIterator implements Iterator<K> {

        private boolean state=false;

        public KeyIteratorInitializable(int segmentIndex, int tableIndex){
            state = init(segmentIndex,tableIndex);
            if(state){
//                advance();
            }
        }

        @Override
        public boolean hasNext() {
            return state && super.hasNext();
        }

        public K next() {
            if(state) {
                return super.nextEntry().key();
            }
            return null;
        }

        public K nextElement() {
            return next();
        }
    }

    class InitializableIterator extends HashIterator{

        boolean init(int segmentIndex, int tableIndex){
            nextSegmentIndex = segmentIndex < segments.length ? segmentIndex : segments.length-1;
            if(nextSegmentIndex < 0){
                return false;
            }
            final Segment<K, V> segment = segments[nextSegmentIndex];
            if(tableIndex >=0 && tableIndex < segment.table.length) {
                this.currentTable = segment.table;
                nextTableIndex = tableIndex;
                if ((nextEntry = currentTable[nextTableIndex--]) != null)
                    return true;
            }

            while (nextTableIndex >= 0) {
                if ((nextEntry = currentTable[nextTableIndex--]) != null)
                    return true;
            }

            while (nextSegmentIndex >= 0 && nextSegmentIndex < segments.length) {
                Segment<K, V> seg = segments[nextSegmentIndex--];
                if (seg.count != 0) {
                    this.currentTable = seg.table;
                    for (int j = currentTable.length - 1; j >= 0; --j) {
                        if ((nextEntry = currentTable[j]) != null) {
                            nextTableIndex = j - 1;
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }


}
