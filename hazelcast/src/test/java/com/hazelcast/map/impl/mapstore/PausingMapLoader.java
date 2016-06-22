package com.hazelcast.map.impl.mapstore;

import com.hazelcast.core.IFunction;
import com.hazelcast.core.MapLoader;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterableUtil;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

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

        return IterableUtil.map(allKeys, new IFunction<K, K>() {
            @Override
            public K apply(K key) {
                if (counter++ == pauseAt) {
                    pause();
                }
                return key;
            }
        });
    }

    private void pause() {
        try {
            pauseLatch.countDown();
            resumeLatch.await();
        } catch (InterruptedException e) {
            ExceptionUtil.rethrow(e);
        }
    }

    public void awaitPause() {
        try {
            pauseLatch.await();
        } catch (InterruptedException e) {
            ExceptionUtil.rethrow(e);
        }
    }

    public void resume() {
        resumeLatch.countDown();
    }
}
