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

package com.hazelcast.map.writebehind.store;

import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapService;
import com.hazelcast.map.writebehind.DelayedEntry;
import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Manages store operations.
 */
class DefaultMapStoreManager implements MapStoreManager<DelayedEntry> {

    private static final int RETRY_TIMES_OF_A_FAILED_STORE_OPERATION = 3;

    private static final int RETRY_STORE_AFTER_WAIT_SECONDS = 1;

    private final MapService mapService;

    private final MapStore mapStore;

    private final List<StoreListener> storeListeners;

    private final ILogger logger;

    DefaultMapStoreManager(MapService mapService, MapStore mapStore, List<StoreListener> listeners) {
        if (listeners == null) {
            throw new IllegalArgumentException("First, set store listeners.");
        }
        this.mapService = mapService;
        this.mapStore = mapStore;
        this.storeListeners = listeners;
        this.logger = mapService.getNodeEngine().getLogger(DefaultMapStoreManager.class);
    }

    @Override
    public Map<Integer, List<DelayedEntry>> process(List<DelayedEntry> delayedEntries) {
        if (delayedEntries == null || delayedEntries.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<Integer, List<DelayedEntry>> failsPerPartition = new HashMap<Integer, List<DelayedEntry>>();
        final List<DelayedEntry> entriesToProcess = new ArrayList<DelayedEntry>();
        StoreOperationType operationType = null;
        StoreOperationType previousOperationType;
        // process entries by preserving order.
        for (final DelayedEntry<Data, Object> entry : delayedEntries) {
            previousOperationType = operationType;
            if (entry.getValue() == null) {
                operationType = StoreOperationType.DELETE;
            } else {
                operationType = StoreOperationType.WRITE;
            }
            if (previousOperationType != null && !previousOperationType.equals(operationType)) {
                final List<DelayedEntry> failures = callHandler(entriesToProcess, previousOperationType);
                addToFails(failures, failsPerPartition);
                entriesToProcess.clear();
            }
            entriesToProcess.add(entry);
        }
        final List<DelayedEntry> failures = callHandler(entriesToProcess, operationType);
        addToFails(failures, failsPerPartition);
        entriesToProcess.clear();
        return failsPerPartition;
    }

    private void addToFails(List<DelayedEntry> fails, Map<Integer, List<DelayedEntry>> failsPerPartition) {
        if (fails == null || fails.isEmpty()) {
            return;
        }
        for (DelayedEntry entry : fails) {
            final int partitionId = entry.getPartitionId();
            List<DelayedEntry> delayedEntriesPerPartition = failsPerPartition.get(partitionId);
            if (delayedEntriesPerPartition == null) {
                delayedEntriesPerPartition = new ArrayList<DelayedEntry>();
                failsPerPartition.put(partitionId, delayedEntriesPerPartition);
            }
            delayedEntriesPerPartition.add(entry);
        }
    }

    /**
     * Decides how entries should be passed to handlers.
     * It passes entries to handler's single or batch handling
     * methods.
     *
     * @param delayedEntries sorted entries to be processed.
     * @return failed entry list if any.
     */
    private List<DelayedEntry> callHandler(Collection<DelayedEntry> delayedEntries, StoreOperationType operationType) {
        final int size = delayedEntries.size();
        if (size == 0) {
            return Collections.emptyList();
        }
        if (size == 1) {
            final Iterator<DelayedEntry> iterator = delayedEntries.iterator();
            final DelayedEntry delayedEntry = iterator.next();
            return callSingleStoreWithListeners(delayedEntry, operationType);
        }
        final DelayedEntry[] delayedEntriesArray = delayedEntries.toArray(new DelayedEntry[delayedEntries.size()]);
        final Map<Object, DelayedEntry> batchMap = prepareBatchMap(delayedEntriesArray);

        // if all batch is on same key, call single store.
        if (batchMap.size() == 1) {
            final DelayedEntry delayedEntry = delayedEntriesArray[delayedEntriesArray.length - 1];
            return callSingleStoreWithListeners(delayedEntry, operationType);
        }
        final List<DelayedEntry> failedEntryList = callBatchStoreWithListeners(batchMap, operationType);
        final List<DelayedEntry> failedTries = new ArrayList<DelayedEntry>();
        for (DelayedEntry entry : failedEntryList) {
            final Collection<DelayedEntry> tmpFails = callSingleStoreWithListeners(entry, operationType);
            failedTries.addAll(tmpFails);
        }
        return failedTries;
    }

    private Map prepareBatchMap(DelayedEntry[] delayedEntries) {
        final Map<Object, DelayedEntry> batchMap = new HashMap<Object, DelayedEntry>();
        final int length = delayedEntries.length;
        // process in reverse order since we do want to process
        // last store operation on a specific key
        for (int i = length - 1; i >= 0; i--) {
            final DelayedEntry delayedEntry = delayedEntries[i];
            final Object key = delayedEntry.getKey();
            if (!batchMap.containsKey(key)) {
                batchMap.put(key, delayedEntry);
            }
        }
        return batchMap;
    }

    /**
     * @param entry delayed entry to be stored.
     * @return failed entry list if any.
     */
    private List<DelayedEntry> callSingleStoreWithListeners(final DelayedEntry entry,
                                                            final StoreOperationType operationType) {
        return retryCall(new RetryTask<DelayedEntry>() {
            private List<DelayedEntry> failedDelayedEntries = Collections.emptyList();

            @Override
            public boolean run() throws Exception {
                callBeforeStoreListeners(entry);
                final Object key = toObject(entry.getKey());
                final Object value = toObject(entry.getValue());
                boolean result = operationType.processSingle(key, value, mapStore);
                callAfterStoreListeners(entry);
                return result;
            }

            /**
             * Call when store failed.
             */
            @Override
            public List<DelayedEntry> failedList() {
                failedDelayedEntries = Collections.singletonList(entry);
                return failedDelayedEntries;
            }
        });
    }

    private Map convertToObject(Map<Object, DelayedEntry> batchMap) {
        final Map map = new HashMap();
        for (DelayedEntry entry : batchMap.values()) {
            final Object key = mapService.toObject(entry.getKey());
            final Object value = mapService.toObject(entry.getValue());
            map.put(key, value);
        }
        return map;
    }

    /**
     * @param batchMap contains batched delayed entries.
     * @return failed entry list if any.
     */
    private List<DelayedEntry> callBatchStoreWithListeners(final Map<Object, DelayedEntry> batchMap,
                                                           final StoreOperationType operationType) {
        return retryCall(new RetryTask<DelayedEntry>() {
            private List<DelayedEntry> failedDelayedEntries = Collections.emptyList();

            @Override
            public boolean run() throws Exception {
                callBeforeStoreListeners(batchMap.values());
                final Map map = convertToObject(batchMap);
                final boolean result = operationType.processBatch(map, mapStore);
                callAfterStoreListeners(batchMap.values());
                return result;
            }

            /**
             * Call when store failed.
             */
            @Override
            public List<DelayedEntry> failedList() {
                failedDelayedEntries = new ArrayList<DelayedEntry>(batchMap.values().size());
                failedDelayedEntries.addAll(batchMap.values());
                return failedDelayedEntries;
            }
        });
    }

    private Object toObject(Object o) {
        return mapService.toObject(o);
    }


    private void callBeforeStoreListeners(DelayedEntry entry) {
        for (StoreListener listener : storeListeners) {
            listener.beforeStore(StoreEvent.createStoreEvent(entry));
        }
    }

    private void callAfterStoreListeners(DelayedEntry entry) {
        for (StoreListener listener : storeListeners) {
            listener.afterStore(StoreEvent.createStoreEvent(entry));
        }
    }

    @Override
    public void callBeforeStoreListeners(Collection<DelayedEntry> entries) {
        for (DelayedEntry entry : entries) {
            callBeforeStoreListeners(entry);
        }
    }

    @Override
    public void callAfterStoreListeners(Collection<DelayedEntry> entries) {
        for (DelayedEntry entry : entries) {
            callAfterStoreListeners(entry);
        }
    }

    private List<DelayedEntry> retryCall(RetryTask task) {
        boolean result = false;
        Throwable throwable = null;
        int k = 0;
        for (; k < RETRY_TIMES_OF_A_FAILED_STORE_OPERATION; k++) {
            try {
                result = task.run();
            } catch (Throwable t) {
                throwable = t;
            }
            if (!result) {
                sleepSeconds(RETRY_STORE_AFTER_WAIT_SECONDS);
            } else {
                break;
            }
        }
        // retry occurred.
        if (k > 0) {
            final String msg = String.format("Store operation failed and retries %s",
                    result ? "succeeded." : "failed too.");
            logger.warning(msg, throwable);
            if (!result) {
                return task.failedList();
            }
        }
        return Collections.emptyList();
    }

    /**
     * Main contract for retry operations.
     *
     * @param <T> the type of object to be processed in this task.
     */
    private interface RetryTask<T> {

        boolean run() throws Exception;

        List<T> failedList();
    }

    private void sleepSeconds(long secs) {
        try {
            TimeUnit.SECONDS.sleep(secs);
        } catch (InterruptedException e) {
            logger.warning(e);
        }
    }

    /**
     * Used to group store operations.
     */
    private enum StoreOperationType {

        DELETE {
            @Override
            boolean processSingle(Object key, Object value, MapStore mapStore) {
                mapStore.delete(key);
                return true;
            }

            @Override
            boolean processBatch(Map map, MapStore mapStore) {
                mapStore.deleteAll(map.keySet());
                return true;
            }
        },

        WRITE {
            @Override
            boolean processSingle(Object key, Object value, MapStore mapStore) {
                mapStore.store(key, value);
                return true;
            }

            @Override
            boolean processBatch(Map map, MapStore mapStore) {
                mapStore.storeAll(map);
                return true;
            }
        };

        abstract boolean processSingle(Object key, Object value, MapStore mapStore);

        abstract boolean processBatch(Map map, MapStore mapStore);
    }

}
