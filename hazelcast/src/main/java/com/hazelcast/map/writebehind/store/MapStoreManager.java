package com.hazelcast.map.writebehind.store;

import java.util.Collection;
import java.util.Map;

/**
 * Map store managers main contract.
 * @param <E> Type of object which is going to be processed by map store.
 */
public interface MapStoreManager<E> {

    void process(Collection<E> delayedEntries, Map<Integer, Collection<E>> failedsPerPartition);

    /**
     * TODO this seems to belong a configuration thingy.
     * @param reduceStoreOperationsIfPossible combine operations on same key.
     */
    void setReduceStoreOperationsIfPossible(boolean reduceStoreOperationsIfPossible);

    void callAfterStoreListeners(Collection<E> entries);

    void callBeforeStoreListeners(Collection<E> entries);
}
