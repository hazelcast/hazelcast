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

package com.hazelcast.map.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.operation.KeyLoadStatusOperation;
import com.hazelcast.map.impl.operation.KeyLoadStatusOperationFactory;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.operation.TriggerLoadIfNeededOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
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

import static com.hazelcast.logging.Logger.getLogger;
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

    private ILogger logger;

    private String mapName;
    private OperationService opService;
    private IPartitionService partitionService;
    private final ClusterService clusterService;
    private IFunction<Object, Data> toData;
    private ExecutionService execService;
    private CoalescingDelayedTrigger delayedTrigger;

    private int maxSizePerNode;
    private int maxBatch;
    private int mapNamePartition;
    private int partitionId;
    private boolean hasBackup;

    private LoadFinishedFuture keyLoadFinished = new LoadFinishedFuture(true);
    private MapOperationProvider operationProvider;

    /**
     * Role of this MapKeyLoader
     **/
    enum Role {
        NONE,
        /**
         * Sends out keys to all other partitions
         **/
        SENDER,
        /**
         * Receives keys from sender
         **/
        RECEIVER,
        /**
         * Restarts sending if SENDER fails
         **/
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

    public MapKeyLoader(String mapName, OperationService opService, IPartitionService ps,
                        ClusterService clusterService, ExecutionService execService, IFunction<Object, Data> serialize) {
        this.mapName = mapName;
        this.opService = opService;
        this.partitionService = ps;
        this.clusterService = clusterService;
        this.toData = serialize;
        this.execService = execService;
        this.logger = getLogger(MapKeyLoader.class);
    }

    public Future startInitialLoad(MapStoreContext mapStoreContext, int partitionId) {

        this.partitionId = partitionId;
        this.mapNamePartition = partitionService.getPartitionId(toData.apply(mapName));
        Role newRole = calculateRole();

        role.nextOrStay(newRole);
        state.next(State.LOADING);

        if (logger.isFinestEnabled()) {
            logger.finest("startInitialLoad invoked " + getStateMessage());
        }

        switch (newRole) {
            case SENDER:
                return sendKeys(mapStoreContext, false);
            case SENDER_BACKUP:
            case RECEIVER:
                return triggerLoading();
            default:
                return keyLoadFinished;
        }
    }

    private Role calculateRole() {
        boolean isPartitionOwner = partitionService.isPartitionOwner(partitionId);
        boolean isMapNamePartition = partitionId == mapNamePartition;
        boolean isMapNamePartitionFirstReplica = false;
        if (hasBackup && isMapNamePartition) {
            IPartition partition = partitionService.getPartition(partitionId);
            Address firstReplicaAddress = partition.getReplicaAddress(1);
            Member member = clusterService.getMember(firstReplicaAddress);
            if (member != null) {
                isMapNamePartitionFirstReplica = member.localMember();
            }
        }
        return assignRole(isPartitionOwner, isMapNamePartition, isMapNamePartitionFirstReplica);
    }

    /**
     * Sends keys to all partitions in batches.
     */
    public Future<?> sendKeys(final MapStoreContext mapStoreContext, final boolean replaceExistingValues) {

        if (keyLoadFinished.isDone()) {

            keyLoadFinished = new LoadFinishedFuture();

            Future<Boolean> sent = execService.submit(MAP_LOAD_ALL_KEYS_EXECUTOR, new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    sendKeysInBatches(mapStoreContext, replaceExistingValues);
                    return false;
                }
            });

            execService.asCompletableFuture(sent).andThen(keyLoadFinished);
        }

        return keyLoadFinished;
    }

    /**
     * Check if loaded on SENDER partition. Triggers key loading if it hadn't started
     */
    public Future triggerLoading() {

        if (keyLoadFinished.isDone()) {

            keyLoadFinished = new LoadFinishedFuture();

            // side effect -> just trigger load on SENDER_BACKUP ID SENDER died
            execService.execute(MAP_LOAD_ALL_KEYS_EXECUTOR, new Runnable() {
                @Override
                public void run() {
                    // checks if loading has finished and triggers loading in case SENDER died and SENDER_BACKUP took over.
                    Operation op = new TriggerLoadIfNeededOperation(mapName);
                    opService.<Boolean>invokeOnPartition(SERVICE_NAME, op, mapNamePartition)
                            // required since loading may be triggered after migration
                            // and in this case the callback is the only way to get to know if the key load finished or not.
                            .andThen(loadingFinishedCallback());
                }
            });
        }

        return keyLoadFinished;
    }

    private ExecutionCallback<Boolean> loadingFinishedCallback() {
        return new ExecutionCallback<Boolean>() {
            @Override
            public void onResponse(Boolean loadingFinished) {
                if (loadingFinished) {
                    updateLocalKeyLoadStatus(null);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                updateLocalKeyLoadStatus(t);
            }
        };
    }

    private void updateLocalKeyLoadStatus(Throwable t) {
        Operation op = new KeyLoadStatusOperation(mapName, t);
        // This updates the local record store on the partition thread.
        // If invoked by the SENDER_BACKUP however it's the replica index has to be set to 1, otherwise
        // it will be a remote call to the SENDER who is the owner of the given partitionId.
        if (hasBackup && role.is(Role.SENDER_BACKUP)) {
            opService.createInvocationBuilder(SERVICE_NAME, op, partitionId).setReplicaIndex(1).invoke();
        } else {
            opService.createInvocationBuilder(SERVICE_NAME, op, partitionId).invoke();
        }
    }

    public Future<?> startLoading(MapStoreContext mapStoreContext, boolean replaceExistingValues) {

        role.nextOrStay(Role.SENDER);

        if (state.is(State.LOADING)) {
            return keyLoadFinished;
        }
        state.next(State.LOADING);

        return sendKeys(mapStoreContext, replaceExistingValues);
    }

    public void trackLoading(boolean lastBatch, Throwable exception) {
        if (lastBatch) {
            state.nextOrStay(State.LOADED);
            if (exception != null) {
                keyLoadFinished.setResult(exception);
            } else {
                keyLoadFinished.setResult(true);
            }
        } else if (state.is(State.LOADED)) {
            state.next(State.LOADING);
        }
    }

    /**
     * Triggers key loading on SENDER if it hadn't started. Delays triggering if invoked multiple times.
     **/
    public void triggerLoadingWithDelay() {
        if (delayedTrigger == null) {
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    Operation op = new TriggerLoadIfNeededOperation(mapName);
                    opService.invokeOnPartition(SERVICE_NAME, op, mapNamePartition);
                }
            };
            delayedTrigger = new CoalescingDelayedTrigger(execService, LOADING_TRIGGER_DELAY, LOADING_TRIGGER_DELAY, runnable);
        }

        delayedTrigger.executeWithDelay();
    }

    // If this gets invoked on SENDER BACKUP it means the SENDER died and SENDER BACKUP takes over.
    public boolean shouldDoInitialLoad() {

        if (role.is(Role.SENDER_BACKUP)) {
            // was backup. become primary sender
            role.next(Role.SENDER);

            if (state.is(State.LOADING)) {
                // previous loading was in progress. cancel and start from scratch
                state.next(State.NOT_LOADED);
                keyLoadFinished.setResult(false);
            }
        }

        return state.is(State.NOT_LOADED);
    }

    private void sendKeysInBatches(MapStoreContext mapStoreContext, boolean replaceExistingValues) throws Exception {

        if (logger.isFinestEnabled()) {
            logger.finest("sendKeysInBatches invoked " + getStateMessage());
        }

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
            FutureUtil.waitForever(futures);

        } catch (Exception caught) {
            loadError = caught;
        } finally {
            sendKeyLoadCompleted(clusterSize, loadError);

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

            MapOperation op = operationProvider.createLoadAllOperation(mapName, keys, replaceExistingValues);

            InternalCompletableFuture<Object> future = opService.invokeOnPartition(SERVICE_NAME, op, partitionId);
            futures.add(future);
        }
        return futures;
    }

    private void sendKeyLoadCompleted(int clusterSize, Throwable exception) throws Exception {
        // Notify SENDER first - reason why this is so important:
        // Someone may do map.get(other_nodes_key) and when it finishes do map.loadAll
        // The problem is that map.get may finish earlier than then overall loading on the SENDER due to the fact
        // that the LoadStatusOperation may first reach the node that did map.get and not the SENDER.
        // The SENDER will be then in the LOADING status, thus the loadAll call will be ignored.
        // it happens only if all LoadAllOperation finish before the sendKeyLoadCompleted is started (test case, little data)
        // Fixes https://github.com/hazelcast/hazelcast/issues/5453
        List<Future> futures = new ArrayList<Future>();
        Operation senderStatus = new KeyLoadStatusOperation(mapName, exception);
        Future senderFuture = opService.createInvocationBuilder(SERVICE_NAME, senderStatus, mapNamePartition)
                .setReplicaIndex(0).invoke();
        futures.add(senderFuture);

        // notify SENDER_BACKUP
        if (hasBackup && clusterSize > 1) {
            Operation senderBackupStatus = new KeyLoadStatusOperation(mapName, exception);
            Future senderBackupFuture = opService.createInvocationBuilder(SERVICE_NAME, senderBackupStatus, mapNamePartition)
                    .setReplicaIndex(1).invoke();
            futures.add(senderBackupFuture);
        }

        // Blocks until finished on SENDER & SENDER_BACKUP PARTITIONS
        // We need to wait for these operation to finished before the map-key-loader returns from the call
        // otherwise the loading won't be finished on SENDER and SENDER_BACKUP but the user may be able to call loadAll which
        // will be ignored since the SENDER and SENDER_BACKUP are still loading.
        FutureUtil.waitForever(futures);

        // INVOKES AND BLOCKS UNTIL FINISHED on ALL PARTITIONS (SENDER AND SENDER BACKUP WILL BE REPEATED)
        // notify all partitions about loading status: finished or exception encountered
        opService.invokeOnAllPartitions(SERVICE_NAME, new KeyLoadStatusOperationFactory(mapName, exception));
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

    public void setMapOperationProvider(MapOperationProvider operationProvider) {
        this.operationProvider = operationProvider;
    }

    public boolean isKeyLoadFinished() {
        return keyLoadFinished.isDone();
    }

    public void promoteToLoadedOnMigration() {
        // The state machine cannot skip states so we need to promote to loaded step by step
        state.next(State.LOADING);
        state.next(State.LOADED);
    }

    private String getStateMessage() {
        return "on partitionId=" + partitionId + " on " + clusterService.getThisAddress() + " role=" + role
                + " state=" + state;
    }

    private static final class LoadFinishedFuture extends AbstractCompletableFuture<Boolean>
            implements ExecutionCallback<Boolean> {

        private LoadFinishedFuture(Boolean result) {
            this();
            setResult(result);
        }

        private LoadFinishedFuture() {
            super((Executor) null, getLogger(LoadFinishedFuture.class));
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
                setResult(true);
            }
            // if not loaded yet we wait for the last batch to arrive
        }

        @Override
        public void onFailure(Throwable t) {
            setResult(t);
        }

        @Override
        protected boolean shouldCancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        protected void setResult(Object result) {
            super.setResult(result);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{done=" + isDone() + "}";
        }
    }

}
