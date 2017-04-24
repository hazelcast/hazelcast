/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl;

import com.hazelcast.cache.ICache;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import javax.cache.expiry.ExpiryPolicy;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class CacheDecoratorTest {

    private ICache<String, String> cacheMock;
    private CacheDecorator deco;

    @Before
    public void before() {
        cacheMock = mock(ICache.class);
        deco = new CacheDecorator(cacheMock, mock(JetInstance.class));
    }

    @Test
    public void getAsync() {
        deco.getAsync("x");
        Mockito.verify(cacheMock).getAsync("x");
        deco.getAsync("x", null);
        Mockito.verify(cacheMock).getAsync("x", null);
    }

    @Test
    public void putAsync() {
        deco.putAsync("x", "x");
        Mockito.verify(cacheMock).putAsync("x", "x");
        deco.putAsync("x", "x", null);
        Mockito.verify(cacheMock).putAsync("x", "x", null);
    }

    @Test
    public void putIfAbsentAsync() {
        deco.putIfAbsentAsync("x", "x");
        Mockito.verify(cacheMock).putIfAbsentAsync("x", "x");
        deco.putIfAbsentAsync("x", "x", null);
        Mockito.verify(cacheMock).putIfAbsentAsync("x", "x", null);
    }

    @Test
    public void getAndPutAsync() {
        deco.getAndPutAsync("x", "x");
        Mockito.verify(cacheMock).getAndPutAsync("x", "x");
        deco.getAndPutAsync("x", "x", null);
        Mockito.verify(cacheMock).getAndPutAsync("x", "x", null);
    }

    @Test
    public void removeAsync() {
        deco.removeAsync("x");
        Mockito.verify(cacheMock).removeAsync("x");
        deco.removeAsync("x", "x");
        Mockito.verify(cacheMock).removeAsync("x", "x");
    }

    @Test
    public void getAndRemoveAsync() {
        deco.getAndRemoveAsync("x");
        Mockito.verify(cacheMock).getAndRemoveAsync("x");
    }

    @Test
    public void replaceAsync() {
        deco.replaceAsync("x", "y");
        Mockito.verify(cacheMock).replaceAsync("x", "y");
        deco.replaceAsync("x", "y", null);
        Mockito.verify(cacheMock).replaceAsync("x", "y", (ExpiryPolicy) null);
        deco.replaceAsync("x", "x", "y");
        Mockito.verify(cacheMock).replaceAsync("x", "x", "y");
        deco.replaceAsync("x", "x", "y", null);
        Mockito.verify(cacheMock).replaceAsync("x", "x", "y", null);
    }

    @Test
    public void getAndReplaceAsync() {
        deco.getAndReplaceAsync("x", "x");
        Mockito.verify(cacheMock).getAndReplaceAsync("x", "x");
        deco.getAndReplaceAsync("x", "x", null);
        Mockito.verify(cacheMock).getAndReplaceAsync("x", "x", null);
    }

    @Test
    public void get() {
        deco.get("x");
        Mockito.verify(cacheMock).get("x");
        deco.get("x", null);
        Mockito.verify(cacheMock).get("x", null);
    }

    @Test
    public void getAll() {
        Set<String> keys = emptySet();
        deco.getAll(keys);
        Mockito.verify(cacheMock).getAll(keys);
        deco.getAll(keys, null);
        Mockito.verify(cacheMock).getAll(keys, null);
    }

    @Test
    public void put() {
        deco.put("x", "x");
        Mockito.verify(cacheMock).put("x", "x");
        deco.put("x", "x", null);
        Mockito.verify(cacheMock).put("x", "x", null);
    }

    @Test
    public void getAndPut() {
        deco.getAndPut("x", "x");
        Mockito.verify(cacheMock).getAndPut("x", "x");
        deco.getAndPut("x", "x", null);
        Mockito.verify(cacheMock).getAndPut("x", "x", null);
    }

    @Test
    public void putAll() {
        Map<String, String> map = emptyMap();
        deco.putAll(map);
        Mockito.verify(cacheMock).putAll(map);
        deco.putAll(map, null);
        Mockito.verify(cacheMock).putAll(map, null);
    }

    @Test
    public void putIfAbsent() {
        deco.putIfAbsent("x", "x");
        Mockito.verify(cacheMock).putIfAbsent("x", "x");
        deco.putIfAbsent("x", "x", null);
        Mockito.verify(cacheMock).putIfAbsent("x", "x", null);
    }

    @Test
    public void replace() {
        deco.replace("x", "y");
        Mockito.verify(cacheMock).replace("x", "y");
        deco.replace("x", "y", null);
        Mockito.verify(cacheMock).replace("x", "y", (ExpiryPolicy) null);
        deco.replace("x", "x", "y");
        Mockito.verify(cacheMock).replace("x", "x", "y");
        deco.replace("x", "x", "y", null);
        Mockito.verify(cacheMock).replace("x", "x", "y", null);
    }

    @Test
    public void getAndReplace() {
        deco.getAndReplace("x", "x");
        Mockito.verify(cacheMock).getAndReplace("x", "x");
        deco.getAndReplace("x", "x", null);
        Mockito.verify(cacheMock).getAndReplace("x", "x", null);
    }

    @Test
    public void destroy() {
        deco.destroy();
        Mockito.verify(cacheMock).destroy();
    }

    @Test
    public void isDestroyed() {
        deco.isDestroyed();
        Mockito.verify(cacheMock).isDestroyed();
    }

    @Test
    public void getLocalCacheStatistics() {
        deco.getLocalCacheStatistics();
        Mockito.verify(cacheMock).getLocalCacheStatistics();
    }

    @Test
    public void addPartitionLostListener() {
        deco.addPartitionLostListener(null);
        Mockito.verify(cacheMock).addPartitionLostListener(null);
    }

    @Test
    public void removePartitionLostListener() {
        deco.removePartitionLostListener(null);
        Mockito.verify(cacheMock).removePartitionLostListener(null);
    }

    @Test
    public void iterator() {
        deco.iterator();
        Mockito.verify(cacheMock).iterator();
        deco.iterator(1);
        Mockito.verify(cacheMock).iterator(1);
    }

    @Test
    public void containsKey() {
        deco.containsKey("x");
        Mockito.verify(cacheMock).containsKey("x");
    }

    @Test
    public void loadAll() {
        Set<String> keys = emptySet();
        deco.loadAll(keys, false, null);
        Mockito.verify(cacheMock).loadAll(keys, false, null);
    }

    @Test
    public void remove() {
        deco.remove("x");
        Mockito.verify(cacheMock).remove("x");
        deco.remove("x", "x");
        Mockito.verify(cacheMock).remove("x", "x");
    }

    @Test
    public void getAndRemove() {
        deco.getAndRemove("x");
        Mockito.verify(cacheMock).getAndRemove("x");
    }

    @Test
    public void removeAll() {
        deco.removeAll();
        Mockito.verify(cacheMock).removeAll();
        Set<String> keys = emptySet();
        deco.removeAll(keys);
        Mockito.verify(cacheMock).removeAll(keys);
    }

    @Test
    public void clear() {
        deco.clear();
        Mockito.verify(cacheMock).clear();
    }

    @Test
    public void getConfiguration() {
        deco.getConfiguration(null);
        Mockito.verify(cacheMock).getConfiguration(null);
    }

    @Test
    public void invoke() {
        deco.invoke("x", null);
        Mockito.verify(cacheMock).invoke("x", null);
    }

    @Test
    public void invokeAll() {
        Set<String> keys = emptySet();
        deco.invokeAll(keys, null);
        Mockito.verify(cacheMock).invokeAll(keys, null);
    }

    @Test
    public void getName() {
        deco.getName();
        Mockito.verify(cacheMock).getName();
    }

    @Test
    public void getCacheManager() {
        deco.getCacheManager();
        Mockito.verify(cacheMock).getCacheManager();
    }

    @Test
    public void close() {
        deco.close();
        Mockito.verify(cacheMock).close();
    }

    @Test
    public void isClosed() {
        deco.isClosed();
        Mockito.verify(cacheMock).isClosed();
    }

    @Test
    public void unwrap() {
        deco.unwrap(null);
        Mockito.verify(cacheMock).unwrap(null);
    }

    @Test
    public void registerCacheEntryListener() {
        deco.registerCacheEntryListener(null);
        Mockito.verify(cacheMock).registerCacheEntryListener(null);
    }

    @Test
    public void deregisterCacheEntryListener() {
        deco.deregisterCacheEntryListener(null);
        Mockito.verify(cacheMock).deregisterCacheEntryListener(null);
    }

    @Test
    public void forEach() {
        deco.forEach(null);
        Mockito.verify(cacheMock).forEach(null);
    }

    @Test
    public void spliterator() {
        deco.spliterator();
        Mockito.verify(cacheMock).spliterator();
    }

    @Test
    public void getPrefixedName() {
        deco.getPrefixedName();
        Mockito.verify(cacheMock).getPrefixedName();
    }

    @Test
    public void getPartitionKey() {
        deco.getPartitionKey();
        Mockito.verify(cacheMock).getPartitionKey();
    }

    @Test
    public void getServiceName() {
        deco.getServiceName();
        Mockito.verify(cacheMock).getServiceName();
    }
}
