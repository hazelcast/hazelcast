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

package com.hazelcast.client;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.hazelcast.config.NearCacheConfig;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class GuavaNearCacheImpl<K,V> implements NearCache<K,V> {
    LoadingCache<K, Optional<V>> cache;
    private MapClientProxy<K, V> map;
    
    public GuavaNearCacheImpl(NearCacheConfig nc, final MapClientProxy<K,V> map) {
        this.map = map;
        CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
        if (nc.getMaxSize() > 0) cacheBuilder.maximumSize(nc.getMaxSize());
        if (nc.getTimeToLiveSeconds() > 0)
            cacheBuilder.expireAfterWrite(nc.getTimeToLiveSeconds(), TimeUnit.SECONDS);
        if (nc.getMaxIdleSeconds() > 0) cacheBuilder.expireAfterAccess(nc.getMaxIdleSeconds(), TimeUnit.SECONDS);
        cache = cacheBuilder.build(new CacheLoader() {
            @Override
            public Object load(Object o) throws Exception {
                try {
                	return Optional.fromNullable(map.get0(o));
                } catch (Exception e) {
                    throw new ExecutionException(e);
                }
            }
        });
    }

    public void invalidate(K key) {
        cache.invalidate(key);
    }

    public V get(K key) {
        try {
            return cache.get(key).orNull();
        } catch (ExecutionException e) {
        	
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            else
                return map.get0(key);
        }
    }
}
