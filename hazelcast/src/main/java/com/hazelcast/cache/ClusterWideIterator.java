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

package com.hazelcast.cache;


import com.hazelcast.cache.operation.CacheKeyIteratorOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class ClusterWideIterator<K,V> implements Iterator<Cache.Entry<K, V>> {

    private int partitionIndex;
    private int segmentIndex=Integer.MAX_VALUE;
    private int tableIndex=-1;

    private int fetchSize;

    private CacheProxy<K,V> cacheProxy;

    private Set<Data> keys=null;

    private Iterator<Data> keysIterator=null;
    private Data nextKey;

    private Data lastKey;

    private boolean hasNextKey=false;
    final SerializationService serializationService;

    public ClusterWideIterator(CacheProxy<K, V> cacheProxy) {
        this.cacheProxy = cacheProxy;

        final NodeEngine engine = cacheProxy.getNodeEngine();
        serializationService = engine.getSerializationService();

        this.partitionIndex = engine.getPartitionService().getPartitionCount()-1;

        //TODO can be made configurable
        this.fetchSize = 100;
    }

    @Override
    public boolean hasNext() {
        if(nextKey == null){
            advance();
        }
        return nextKey != null;
    }

    @Override
    public Cache.Entry<K, V> next() {
        if(hasNext()){
            try {
                final V value = cacheProxy.get(nextKey);
                final K key = serializationService.toObject(nextKey);
                return new CacheEntry<K, V>(key,value);
            } finally {
                advance();
            }
        }
        throw new NoSuchElementException("Iteration ended. Has no more element");
    }

    @Override
    public void remove() {
        if (lastKey == null) {
            throw new IllegalStateException("Must progress to the next entry to remove");
        }
        if(cacheProxy.remove(lastKey)){
            lastKey=null;
        }
    }

    private void advance(){
        if(nextKey != null && keysIterator.hasNext()){
            lastKey = nextKey;
            nextKey = keysIterator.next();
            return;
        }

        while((partitionIndex >= 0) && (tableIndex>=0 || segmentIndex >= 0)){
            fetch();
            if(nextKey != null){
                return;
            }
        }

        //partition content done, proceed to next one
        if(partitionIndex <=0) {
            lastKey = nextKey;
            nextKey = null;
        } else {
            while(partitionIndex > 0){
                if(segmentIndex < 0){
                    segmentIndex=Integer.MAX_VALUE;
                }
                tableIndex = -1;
//            segmentIndex=Integer.MAX_VALUE;
                partitionIndex--;
                fetch();
                if(nextKey != null){
                    return;
                }
            }
        }

    }

    private void fetch(){
        final NodeEngine nodeEngine = cacheProxy.getNodeEngine();
        final Operation op = new CacheKeyIteratorOperation(cacheProxy.getName(),segmentIndex,tableIndex, fetchSize);
        final InternalCompletableFuture<Object> f = nodeEngine.getOperationService()
                .invokeOnPartition(CacheService.SERVICE_NAME, op, partitionIndex);

        final CacheKeyIteratorResult iteratorResult = (CacheKeyIteratorResult) f.getSafely();
        if(iteratorResult != null){
            segmentIndex = iteratorResult.getSegmentIndex();
            tableIndex = iteratorResult.getTableIndex();
            keys = iteratorResult.getKeySet();
            keysIterator = keys.iterator();
            if(keysIterator.hasNext()){
                lastKey = nextKey;
                nextKey = this.keysIterator.next();
                return;
            }
        }
        segmentIndex = -1;
        tableIndex = -1;
        nextKey = null;
    }
}
