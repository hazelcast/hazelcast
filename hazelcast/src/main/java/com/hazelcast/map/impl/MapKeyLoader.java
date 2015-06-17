/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.MapLoader;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.operation.LoadAllOperation;
import com.hazelcast.map.impl.operation.LoadStatusOperation;
import com.hazelcast.map.impl.operation.PartitionCheckIfLoadedOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.StateMachine;
import com.hazelcast.util.scheduler.CoalescingDelayedTrigger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.map.impl.MapKeyLoaderUtil.assignRole;
import static com.hazelcast.map.impl.MapKeyLoaderUtil.toBatches;
import static com.hazelcast.map.impl.MapKeyLoaderUtil.toPartition;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.spi.ExecutionService.MAP_LOAD_ALL_KEYS_EXECUTOR;
import static com.hazelcast.util.IterableUtil.limit;
import static com.hazelcast.util.IterableUtil.map;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Loads keys from a {@link MapLoader} and sends them to all partitions for loading
 */
public class MapKeyLoader {

    private static final long LOADING_TRIGGER_DELAY = SECONDS.toMillis(5);
    private static final long KEY_DISTRIBUTION_TIMEOUT_MINUTES = 15;

    private String mapName;
    private OperationService opService;
    private InternalPartitionService partitionService;
    private IFunction<Object, Data> toData;
    private ExecutionService execService;
    private CoalescingDelayedTrigger deleayedTrigger;

    private int maxSizePerNode;
    private int maxBatch;
    private int mapNamePartition;
    private boolean hasBackup;

    private LoadFinishedFuture loadFinished = new LoadFinishedFuture(true);

    /** Role of this MapKeyLoader **/
    enum Role {
        NONE,
        /** Sends out keys to all other partitions **/
        SENDER,
        /** Receives keys from sender **/
        RECEIVER,
        /** Restarts sending if SENDER fails **/
        SENDER_BACKUP
    }

    enum State {
        NOT_LOADED,
        LOADING,
        LOADED
    }

    private final StateMachine<Role> role = StateMachine.of(Role.NONE)
            .withTransition(Role.NONE, Role.SENDER, Role.RECEIVER, Role.SENDER_BACKUP)
            .withTransition(Role.SENDER_BACKUP, Role.SENDER);

    private final StateMachine<State> state = StateMachine.of(State.NOT_LOADED)
            .withTransition(State.NOT_LOADED, State.LOADING)
            .withTransition(State.LOADING, State.LOADED, State.NOT_LOADED)
            .withTransition(State.LOADED, State.LOADING);

    public MapKeyLoader(String mapName, OperationService opService, InternalPartitionService ps,
            ExecutionService execService, IFunction<Object, Data> serialize) {
        this.mapName = mapName;
        this.opService = opService;
        this.partitionService = ps;
        this.toData = serialize;
        this.execService = execService;
    }

    public Future startInitialLoad(MapStoreContext mapStoreContext, int partitionId) {

        this.mapNamePartition = partitionService.getPartitionId(toData.apply(mapName));
        Role newRole = assignRole(partitionService, mapNamePartition, partitionId);

        role.nextOrStay(newRole);
        state.next(State.LOADING);

        switch(newRole) {
            case SENDER:
                return sendKeys(mapStoreContext, false);
            case SENDER_BACKUP:
            case RECEIVER:
                return triggerLoading();
            default:
                return loadFinished;
        }
    }

    /**
     * Sends keys to all partitions in batches.
     */
    public Future<?> sendKeys(final MapStoreContext mapStoreContext, final boolean replaceExistingValues) {

        if (loadFinished.isDone()) {

            loadFinished = new LoadFinishedFuture();

            Future<Boolean> sent = execService.submit(MAP_LOAD_ALL_KEYS_EXECUTOR, new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    sendKeysInBatches(mapStoreContext, replaceExistingValues);
                    return false;
                }
            });

            execService.asCompletableFuture(sent).andThen(loadFinished);
        }

        return loadFinished;
    }

    /**
     * Check if loaded on SENDER partition. Triggers key loading if it hadn't started
     */
    public Future triggerLoading() {

        if (loadFinished.isDone()) {

            loadFinished = new LoadFinishedFuture();

            execService.execute(MAP_LOAD_ALL_KEYS_EXECUTOR, new Runnable() {
                @Override
                public void run() {
                    Operation op = new PartitionCheckIfLoadedOperation(mapName, true);
                    opService.<Boolean>invokeOnPartition(SERVICE_NAME, op, mapNamePartition)
                        .andThen(ifLoadedCallback());
                }
            });
        }

        return loadFinished;
    }

    public Future<?> startLoading(MapStoreContext mapStoreContext, boolean replaceExistingValues) {

        role.nextOrStay(Role.SENDER);

        if (state.is(State.LOADING)) {
            return loadFinished;
        }
        state.next(State.LOADING);

        return sendKeys(mapStoreContext, replaceExistingValues);
    }

    public void trackLoading(boolean lastBatch, Throwable exception) {
        if (lastBatch) {
            state.nextOrStay(State.LOADED);
            if (exception != null) {
                loadFinished.setResult(exception);
            } else {
                loadFinished.setResult(true);
            }
        } else if (state.is(State.LOADED)) {
            state.next(State.LOADING);
        }
    }

    /** Triggers key loading on SENDER if it hadn't started. Delays triggering if invoked multiple times. **/
    public void triggerLoadingWithDelay() {
        if (deleayedTrigger == null) {
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    Operation op = new PartitionCheckIfLoadedOperation(mapName, true);
                    opService.invokeOnPartition(SERVICE_NAME, op, mapNamePartition);
                }
            };
            deleayedTrigger = new CoalescingDelayedTrigger(execService, LOADING_TRIGGER_DELAY, LOADING_TRIGGER_DELAY, runnable);
        }

        deleayedTrigger.executeWithDelay();
    }

    public boolean shouldDoInitialLoad() {

        if (role.is(Role.SENDER_BACKUP)) {
            // was backup. become primary sender
            role.next(Role.SENDER);

            if (state.is(State.LOADING)) {
                // previous loading was in progress. cancel and start from scratch
                state.next(State.NOT_LOADED);
                loadFinished.setResult(false);
            }
        }

        return state.is(State.NOT_LOADED);
    }

    private void sendKeysInBatches(MapStoreContext mapStoreContext, boolean replaceExistingValues) {

        int clusterSize = partitionService.getMemberPartitionsMap().size();
        Iterator<Object> keys = null;
        Throwable loadError = null;

        try {
            Iterable<Object> allKeys = mapStoreContext.loadAllKeys();
            keys = allKeys.iterator();
            Iterator<Data> dataKeys = map(keys, toData);
            int mapMaxSize = clusterSize * maxSizePerNode;

            if (mapMaxSize > 0) {
                dataKeys = limit(dataKeys, mapMaxSize);
            }

            Iterator<Entry<Integer, Data>> partitionsAndKeys = map(dataKeys, toPartition(partitionService));
            Iterator<Map<Integer, List<Data>>> batches = toBatches(partitionsAndKeys, maxBatch);

            List<Future> futures = new ArrayList<Future>();
            while (batches.hasNext()) {
                Map<Integer, List<Data>> batch = batches.next();
                futures.addAll(sendBatch(batch, replaceExistingValues));
            }

            // This acts as a barrier to prevent re-ordering of key distribution operations (LoadAllOperation)
            // and LoadStatusOperation(s) which indicates all keys were already loaded.
            // Re-ordering of in-flight operations can happen during a partition migration. We are waiting here
            // for all LoadAllOperation(s) to be ACKed by receivers and only then we send them the LoadStatusOperation
            // See https://github.com/hazelcast/hazelcast/issues/4024 for additional details
            FutureUtil.waitWithDeadline(futures, KEY_DISTRIBUTION_TIMEOUT_MINUTES, TimeUnit.MINUTES);
        } catch (Exception caught) {
            loadError = caught;
        } finally {
            sendLoadCompleted(clusterSize, partitionService.getPartitionCount(), replaceExistingValues, loadError);

            if (keys instanceof Closeable) {
                closeResource((Closeable) keys);
            }
        }
    }

    private List<Future> sendBatch(Map<Integer, List<Data>> batch, boolean replaceExistingValues) {
        Set<Entry<Integer, List<Data>>> entries = batch.entrySet();
        List<Future> futures = new ArrayList<Future>(entries.size());
        for (Entry<Integer, List<Data>> e : entries) {
            int partitionId = e.getKey();
            List<Data> keys = e.getValue();
            LoadAllOperation op = new LoadAllOperation(mapName, keys, replaceExistingValues);
            InternalCompletableFuture<Object> future = opService.invokeOnPartition(SERVICE_NAME, op, partitionId);
            futures.add(future);
        }
        return futures;
    }

    private void sendLoadCompleted(int clusterSize, int partitions,
            boolean replaceExistingValues, Throwable exception) {
        for (int partitionId = 0; partitionId < partitions; partitionId++) {
            Operation op = new LoadStatusOperation(mapName, exception);
            opService.invokeOnPartition(SERVICE_NAME, op, partitionId);
        }

        // notify SENDER_BACKUP
        if (hasBackup && clusterSize > 1) {
            Operation op = new LoadStatusOperation(mapName, exception);
            opService.createInvocationBuilder(SERVICE_NAME, op, mapNamePartition).setReplicaIndex(1).invoke();
        }

    }

    public void setMaxBatch(int maxBatch) {
        this.maxBatch = maxBatch;
    }

    public void setMaxSize(int maxSize) {
        this.maxSizePerNode = maxSize;
    }

    public void setHasBackup(boolean hasBackup) {
        this.hasBackup = hasBackup;
    }

    private ExecutionCallback<Boolean> ifLoadedCallback() {
        return new ExecutionCallback<Boolean>() {
            @Override
            public void onResponse(Boolean loaded) {
                if (loaded) {
                    state.nextOrStay(State.LOADED);
                    loadFinished.setResult(true);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                loadFinished.setResult(t);
            }
        };
    }

    private static final class LoadFinishedFuture extends AbstractCompletableFuture<Boolean>
            implements ExecutionCallback<Boolean> {

        private LoadFinishedFuture(Boolean result) {
            this();
            setResult(result);
        }

        private LoadFinishedFuture() {
            super((Executor) null, null);
        }

        @Override
        public Boolean get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
            if (isDone()) {
                return getResult();
            }
            throw new UnsupportedOperationException("Future is not done yet");
        }

        @Override
        public void onResponse(Boolean loaded) {
            if (loaded) {
                setResult(loaded);
            }
            // if not loaded yet we wait for the last batch to arrive
        }

        @Override
        public void onFailure(Throwable t) {
            setResult(t);
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{done=" + isDone() + "}";
        }
    }
}
