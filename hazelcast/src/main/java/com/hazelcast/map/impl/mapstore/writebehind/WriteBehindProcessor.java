package com.hazelcast.map.impl.mapstore.writebehind;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Write behind processors main contract.
 * Responsible for processing map store logic like retries, failures, listeners, batch size etc.
 *
 * @param <E> Type of object which is going to be processed by map store.
 */
public interface WriteBehindProcessor<E> {

    /**
     * Process store operations and returns failed operation per partition map.
     *
     * @param delayedEntries to be written to store.
     * @return failed store operations per partition.
     */
    Map<Integer, List<E>> process(List<E> delayedEntries);

    void callAfterStoreListeners(Collection<E> entries);

    void callBeforeStoreListeners(Collection<E> entries);

    void addStoreListener(StoreListener storeListener);

    Collection flush(WriteBehindQueue queue);

    /**
     * Flush a key directly to map store.
     *
     * @param key to be flushed.
     */
    void flush(E key);
}
