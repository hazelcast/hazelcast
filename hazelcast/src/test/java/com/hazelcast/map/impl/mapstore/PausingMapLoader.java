/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.MapLoader;
import com.hazelcast.internal.util.IterableUtil;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * MapLoader that pauses once while loading keys until resumed using {@link #resume()}
 **/
class PausingMapLoader<K, V> implements MapLoader<K, V> {

    private MapLoader<K, V> delegate;

    private int pauseAt;
    private int counter;
    private CountDownLatch resumeLatch = new CountDownLatch(1);
    private CountDownLatch pauseLatch = new CountDownLatch(1);

    PausingMapLoader(MapLoader<K, V> delegate, int pauseAt) {
        this.delegate = delegate;
        this.pauseAt = pauseAt;
    }

    @Override
    public V load(K key) {
        return delegate.load(key);
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        return delegate.loadAll(keys);
    }

    @Override
    public Iterable<K> loadAllKeys() {
        Iterable<K> allKeys = delegate.loadAllKeys();

        return IterableUtil.map(allKeys, key -> {
            if (counter++ == pauseAt) {
                pause();
            }
            return key;
        });
    }

    private void pause() {
        try {
            pauseLatch.countDown();
            resumeLatch.await();
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    public void awaitPause() {
        try {
            pauseLatch.await();
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    public void resume() {
        resumeLatch.countDown();
    }
}
