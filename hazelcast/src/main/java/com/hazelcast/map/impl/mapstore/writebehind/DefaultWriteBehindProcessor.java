/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.CollectionUtil.isNotEmpty;
import static com.hazelcast.util.MapUtil.createHashMap;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Processes store operations.
 */
class DefaultWriteBehindProcessor extends AbstractWriteBehindProcessor<DelayedEntry> {

    private static final Comparator<DelayedEntry> DELAYED_ENTRY_COMPARATOR = new Comparator<DelayedEntry>() {
        @Override
        public int compare(DelayedEntry o1, DelayedEntry o2) {
            final long s1 = o1.getStoreTime();
            final long s2 = o2.getStoreTime();
            return (s1 < s2) ? -1 : ((s1 == s2) ? 0 : 1);
        }
    };

    private static final int RETRY_TIMES_OF_A_FAILED_STORE_OPERATION = 3;

    private static final int RETRY_STORE_AFTER_WAIT_SECONDS = 1;

    private final List<StoreListener> storeListeners;

    DefaultWriteBehindProcessor(MapStoreContext mapStoreContext) {
        super(mapStoreContext);
        this.storeListeners = new ArrayList<StoreListener>(2);
    }

    @Override
    public Map<Integer, List<DelayedEntry>> process(List<DelayedEntry> delayedEntries) {
        Map<Integer, List<DelayedEntry>> failMap;
        sort(delayedEntries);
        if (writeBatchSize > 1) {
            failMap = doStoreUsingBatchSize(delayedEntries);
        } else {
            failMap = processInternal(delayedEntries);
        }
        return failMap;
    }


    private Map<Integer, List<DelayedEntry>> processInternal(List<DelayedEntry> delayedEntries) {
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
        // if we want to write all store operations on a key into the MapStore, not same as write-coalescing, we don't call
        // batch processing methods e.g., MapStore{#storeAll,#deleteAll}, instead we call methods which process single entries
        // e.g. MapStore{#store,#delete}. This is because MapStore#storeAll requires a Map type in its signature and Map type
        // can only contain one store operation type per key, so only last update on a key can be included when batching.
        // Due to that limitation it is not possible to provide a correct no-write-coalescing write-behind behavior.
        // Under that limitation of current MapStore interface, we are making a workaround and persisting all
        // entries one by one for no-write-coalescing write-behind map-stores and as a result not doing batching
        // when writeCoalescing is false.
        if (size == 1 || !writeCoalescing) {
            return processEntriesOneByOne(delayedEntries, operationType);
        }
        final DelayedEntry[] delayedEntriesArray = delayedEntries.toArray(new DelayedEntry[0]);
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

    private List<DelayedEntry> processEntriesOneByOne(Collection<DelayedEntry> delayedEntries,
                                                      StoreOperationType operationType) {
        List<DelayedEntry> totalFailures = null;
        for (DelayedEntry delayedEntry : delayedEntries) {
            List<DelayedEntry> failures = callSingleStoreWithListeners(delayedEntry, operationType);
            // this `if` is used to initialize totalFailures list, since we don't want unneeded object creation.
            if (isNotEmpty(failures)) {
                if (totalFailures == null) {
                    totalFailures = failures;
                } else {
                    totalFailures.addAll(failures);
                }
            }
        }
        return totalFailures == null ? Collections.EMPTY_LIST : totalFailures;
    }

    private Map prepareBatchMap(DelayedEntry[] delayedEntries) {
        final int length = delayedEntries.length;
        final Map<Object, DelayedEntry> batchMap = createHashMap(length);
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
            public List<DelayedEntry> failureList() {
                List failedDelayedEntries = new ArrayList<DelayedEntry>(1);
                failedDelayedEntries.add(entry);
                return failedDelayedEntries;
            }
        });
    }

    private Map convertToObject(Map<Object, DelayedEntry> batchMap) {
        final Map map = createHashMap(batchMap.size());
        for (DelayedEntry entry : batchMap.values()) {
            final Object key = toObject(entry.getKey());
            final Object value = toObject(entry.getValue());
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
                boolean result;
                try {
                    result = operationType.processBatch(map, mapStore);
                } catch (Exception ex) {
                    Iterator<Object> keys = batchMap.keySet().iterator();
                    while (keys.hasNext()) {
                        if (!map.containsKey(toObject(keys.next()))) {
                            keys.remove();
                        }
                    }
                    throw ex;
                }
                callAfterStoreListeners(batchMap.values());
                return result;
            }

            @Override
            public List<DelayedEntry> failureList() {
                failedDelayedEntries = new ArrayList<DelayedEntry>(batchMap.values().size());
                failedDelayedEntries.addAll(batchMap.values());
                return failedDelayedEntries;
            }
        });
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
    public void addStoreListener(StoreListener listeners) {
        storeListeners.add(listeners);
    }

    @Override
    public void flush(WriteBehindQueue queue) {
        int size = queue.size();

        if (size == 0) {
            return;
        }

        List<DelayedEntry> delayedEntries = new ArrayList<DelayedEntry>(size);
        queue.drainTo(delayedEntries);
        flushInternal(delayedEntries);
    }

    @Override
    public void flush(DelayedEntry entry) {
        final List<DelayedEntry> entries = Collections.singletonList(entry);
        flushInternal(entries);
    }

    private void flushInternal(List<DelayedEntry> delayedEntries) {
        sort(delayedEntries);

        Map<Integer, List<DelayedEntry>> failedStoreOpPerPartition = process(delayedEntries);

        if (failedStoreOpPerPartition.size() > 0) {
            printErrorLog(failedStoreOpPerPartition);
        }
    }

    private void printErrorLog(Map<Integer, List<DelayedEntry>> failsPerPartition) {
        int size = 0;
        final Collection<List<DelayedEntry>> values = failsPerPartition.values();
        for (Collection<DelayedEntry> value : values) {
            size += value.size();
        }
        final String logMessage = String.format("Map store flush operation can not be done for %d entries", size);
        logger.severe(logMessage);
    }

    @Override
    public void callAfterStoreListeners(Collection<DelayedEntry> entries) {
        for (DelayedEntry entry : entries) {
            callAfterStoreListeners(entry);
        }
    }

    /**
     * Store chunk by chunk using write batch size {@link #writeBatchSize}
     *
     * @param sortedDelayedEntries entries to be stored.
     * @return not-stored entries per partition.
     */
    private Map<Integer, List<DelayedEntry>> doStoreUsingBatchSize(List<DelayedEntry> sortedDelayedEntries) {
        final Map<Integer, List<DelayedEntry>> failsPerPartition = new HashMap<Integer, List<DelayedEntry>>();
        int page = 0;
        List<DelayedEntry> delayedEntryList;
        while ((delayedEntryList = getBatchChunk(sortedDelayedEntries, writeBatchSize, page++)) != null) {
            final Map<Integer, List<DelayedEntry>> fails = processInternal(delayedEntryList);
            final Set<Map.Entry<Integer, List<DelayedEntry>>> entries = fails.entrySet();
            for (Map.Entry<Integer, List<DelayedEntry>> entry : entries) {
                final Integer partitionId = entry.getKey();
                final List<DelayedEntry> tmpFailList = entry.getValue();
                List<DelayedEntry> failList = failsPerPartition.get(partitionId);
                if (failList == null || failList.isEmpty()) {
                    failsPerPartition.put(partitionId, tmpFailList);
                    failList = failsPerPartition.get(partitionId);
                }
                failList.addAll(tmpFailList);
            }
        }
        return failsPerPartition;
    }

    private List<DelayedEntry> retryCall(RetryTask task) {
        boolean result = false;
        Exception exception = null;
        int k = 0;
        for (; k < RETRY_TIMES_OF_A_FAILED_STORE_OPERATION; k++) {
            try {
                result = task.run();
            } catch (InterruptedException ex) {
                currentThread().interrupt();
                break;
            } catch (Exception ex) {
                exception = ex;
            }
            if (!result) {
                sleepSeconds(RETRY_STORE_AFTER_WAIT_SECONDS);
            } else {
                break;
            }
        }
        // retry occurred.
        if (k > 0) {
            if (!result) {
                // List of entries which can not be stored for this round. We will readd these entries
                // in front of the relevant partition-write-behind-queues and will indefinitely retry to
                // store them.
                List failureList = task.failureList();
                logger.severe("Number of entries which could not be stored is = [" + failureList.size() + "]"
                        + ", Hazelcast will indefinitely retry to store them", exception);
                return failureList;
            }
        }
        return Collections.emptyList();
    }

    private void sort(List<DelayedEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }

        Collections.sort(entries, DELAYED_ENTRY_COMPARATOR);
    }

    /**
     * Main contract for retry operations.
     *
     * @param <T> the type of object to be processed in this task.
     */
    private interface RetryTask<T> {

        /**
         * Returns {@code true} if this task has successfully run, {@code false} otherwise.
         *
         * @return {@code true} if this task has successfully run.
         * @throws Exception
         */
        boolean run() throws Exception;

        /**
         * Returns failed store operations list.
         *
         * @return failed store operations list.
         */
        List<T> failureList();
    }

    private void sleepSeconds(long secs) {
        try {
            SECONDS.sleep(secs);
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }
    }

}
