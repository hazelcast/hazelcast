package com.hazelcast.map.writebehind.store;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Map store managers main contract.
 * Responsible for processing map store logic.
 *
 * @param <E> Type of object which is going to be processed by map store.
 */
public interface MapStoreManager<E> {

    /**
     * Process store operations and returns failed operation per partition map.
     *
     * @param delayedEntries to be written to store.
     * @return failed store operations per partition.
     */
    Map<Integer, List<E>> process(List<E> delayedEntries);

    void callAfterStoreListeners(Collection<E> entries);

    void callBeforeStoreListeners(Collection<E> entries);
}
