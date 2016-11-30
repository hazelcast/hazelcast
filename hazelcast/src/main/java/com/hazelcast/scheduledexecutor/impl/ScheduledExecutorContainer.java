/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.scheduledexecutor.DuplicateTaskException;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.ScheduledTaskStatistics;
import com.hazelcast.scheduledexecutor.StaleTaskException;
import com.hazelcast.scheduledexecutor.StatefulTask;
import com.hazelcast.scheduledexecutor.impl.operations.ResultReadyNotifyOperation;
import com.hazelcast.scheduledexecutor.impl.operations.SyncStateOperation;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;

public class ScheduledExecutorContainer {

    private final ILogger logger;

    private final String name;

    private final NodeEngine nodeEngine;

    private final InternalExecutionService executionService;

    private final int partitionId;

    private final int durability;

    private final ConcurrentMap<String, ScheduledTaskDescriptor> tasks;

    private final Map<String, BackupTaskDescriptor> backups;

    public ScheduledExecutorContainer(String name, int partitionId, NodeEngine nodeEngine,
                                      int durability) {
        this(name, partitionId, nodeEngine, durability, new ConcurrentHashMap<String, BackupTaskDescriptor>());
    }

    public ScheduledExecutorContainer(String name, int partitionId, NodeEngine nodeEngine,
                                      int durability, Map<String, BackupTaskDescriptor> backups) {
        this.logger = nodeEngine.getLogger(getClass());
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.executionService = (InternalExecutionService) nodeEngine.getExecutionService();
        this.partitionId = partitionId;
        this.durability = durability;
        this.tasks = new ConcurrentHashMap<String, ScheduledTaskDescriptor>();
        this.backups = backups;
    }

    public <V> ScheduledFuture<V> schedule(TaskDefinition definition) {
        checkNotDuplicateTask(definition.getName());
        return createContextAndSchedule(definition);
    }

    public boolean cancel(String taskName, boolean mayInterruptIfRunning) {
        checkNotStaleTask(taskName);

        if (logger.isFineEnabled()) {
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "] Cancelling " + taskName);
        }

        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        return descriptor.getScheduledFuture().cancel(mayInterruptIfRunning);
    }

    public Object get(String taskName)
            throws ExecutionException, InterruptedException {
        checkNotStaleTask(taskName);
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        return descriptor.getScheduledFuture().get();
    }

    public long getDelay(String taskName, TimeUnit unit) {
        checkNotStaleTask(taskName);
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        return descriptor.getScheduledFuture().getDelay(unit);
    }

    public ScheduledTaskStatistics getStatistics(String taskName) {
        checkNotStaleTask(taskName);
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        return descriptor.getStats();
    }

    public boolean isCancelled(String taskName) {
        checkNotStaleTask(taskName);
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        return descriptor.getScheduledFuture().isCancelled();
    }

    public boolean isDone(String taskName) {
        checkNotStaleTask(taskName);
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        return descriptor.getScheduledFuture().isDone();
    }

    public void destroy(String taskName) {
        checkNotStaleTask(taskName);

        if (logger.isFineEnabled()) {
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "] Destroying " + taskName);
        }

        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        ScheduledFuture scheduledFuture = descriptor.getScheduledFuture();
        if (!scheduledFuture.isDone()) {
            scheduledFuture.cancel(true);
        }

        tasks.remove(taskName);
    }

    public void stash(TaskDefinition definition) {
        if (logger.isFineEnabled()) {
            logger.finest("[Backup Scheduler: " + name + "][Partition: " + partitionId + "] Stashing " + definition);
        }

        if (!backups.containsKey(definition.getName())) {
            BackupTaskDescriptor descriptor = new BackupTaskDescriptor(definition);
            backups.put(definition.getName(), descriptor);
        }

        if (logger.isFineEnabled()) {
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "] Stash size: " + backups.size());
        }
    }

    public void unstash(String taskName) {
        backups.remove(taskName);
    }

    public Collection<ScheduledTaskDescriptor> getTasks() {
        return tasks.values();
    }

    public void syncState(String taskName, Map newState) {
        backups.get(taskName).setMasterState(newState);
    }

    public boolean shouldParkGetResult(String taskName) {
        return (tasks.containsKey(taskName) && !tasks.get(taskName).getScheduledFuture().isDone())
                || backups.containsKey(taskName);
    }

    public int getDurability() {
        return durability;
    }

    public String getName() {
        return name;
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public ScheduledTaskHandler offprintHandler(String taskName) {
        return ScheduledTaskHandlerImpl.of(partitionId, getName(), taskName);
    }

    void promoteStash() {
        Iterator<Map.Entry<String, BackupTaskDescriptor>> iterator =
                backups.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, BackupTaskDescriptor> entry = iterator.next();
            BackupTaskDescriptor descriptor = entry.getValue();
            doSchedule(descriptor.getDefinition(), descriptor.getMasterState(), descriptor.getMasterStats());
            iterator.remove();
        }

    }

    Map<String, BackupTaskDescriptor> prepareForReplication() {
        Map<String, BackupTaskDescriptor> replicas = new HashMap<String, BackupTaskDescriptor>();

        Iterator<Map.Entry<String, ScheduledTaskDescriptor>> iterator =
                tasks.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, ScheduledTaskDescriptor> entry = iterator.next();
            ScheduledTaskDescriptor descriptor = entry.getValue();

            try {
                BackupTaskDescriptor replica = new BackupTaskDescriptor(descriptor.getDefinition());
                replica.setMasterState(descriptor.getState().get());
                replica.setMasterStats(descriptor.getStats());
                replicas.put(entry.getKey(), replica);
            } finally {
                descriptor.getLock().set(false);
            }
        }

        return replicas;
    }

    protected <V> ScheduledFuture<V> createContextAndSchedule(TaskDefinition definition) {
        if (logger.isFineEnabled()) {
            logger.fine("[Scheduler: " + name + "][Partition: " + partitionId + "] Scheduling " + definition);
        }

        ScheduledTaskStatisticsImpl stats = new ScheduledTaskStatisticsImpl();
        Map<?, ?> taskState = new HashMap();
        ScheduledTaskDescriptor descriptor = doSchedule(definition, taskState, stats);

        if (logger.isFineEnabled()) {
            logger.fine("[Scheduler: " + name + "][Partition: " + partitionId + "] Queue size: " + tasks.size());
        }

        return (ScheduledFuture<V>) descriptor.getScheduledFuture();
    }

    protected void checkNotDuplicateTask(String taskName) {
        if (tasks.containsKey(taskName)) {
            throw new DuplicateTaskException("There is already a task "
                    + "with the same name '" + taskName + "' in '" + getName() + "'");
        }
    }

    /**
     * State is synced after every run with all replicas.
     * When replicas get promoted, they start of, with the latest state see {@link TaskRunner#init()}
     */
    protected void publishStateToReplicas(String taskName, Map snapshot) {
        int maxAllowedBackupCount = nodeEngine.getPartitionService().getMaxAllowedBackupCount();
        boolean noBackup = partitionId == -1;
        boolean replicationNotRequired = Math.min(maxAllowedBackupCount, getDurability()) == 0 || noBackup;
        if (replicationNotRequired) {
            return;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "][Task: " + taskName + "] "
                    + "Publishing state, to replicas. State: " + snapshot);
        }


        for (int i = 1; i < getDurability() + 1; i++) {
            Operation op = new SyncStateOperation(getName(), taskName, snapshot);
            createInvocationBuilder(op)
                    .setReplicaIndex(i)
                    .setCallTimeout(Long.MAX_VALUE)
                    .invoke();
        }

    }

    protected InvocationBuilder createInvocationBuilder(Operation op) {
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(SERVICE_NAME, op, partitionId);
    }

    private <V> ScheduledTaskDescriptor doSchedule(TaskDefinition<V> definition,
                                                   Map<?, ?> stateSnapshot,
                                                   ScheduledTaskStatisticsImpl stats) {

        ScheduledFuture<?> future;
        TaskRunner runner;
        AtomicReference<Map<?, ?>> taskState = new AtomicReference<Map<?, ?>>(stateSnapshot);
        switch (definition.getType()) {
            case SINGLE_RUN:
                runner = new TaskRunner<V>(definition, taskState, stats);
                future = executionService.scheduleDurable(getName(), (Callable) runner,
                        definition.getInitialDelay(), definition.getUnit());
                break;
            case WITH_REPETITION:
                runner = new TaskRunner<V>(definition, taskState, stats);
                future = executionService.scheduleDurableWithRepetition(getName(),
                        runner, definition.getInitialDelay(), definition.getPeriod(),
                        definition.getUnit());
                break;
            default:
                throw new IllegalArgumentException();
        }

        ScheduledTaskDescriptor descriptor = new ScheduledTaskDescriptor(definition, future, taskState, stats);
        tasks.put(definition.getName(), descriptor);

        return descriptor;
    }

    private void checkNotStaleTask(String taskName) {
        if (!tasks.containsKey(taskName)) {
            throw new StaleTaskException("Task with name " + taskName + " not found. ");
        }
    }

    private class TaskRunner<V> implements Callable<V>, Runnable {

        private final String taskName;

        private final Callable<V> original;

        private final AtomicReference<Map<?, ?>> state;

        private final TaskLifecycleListener lifecycleListener;

        TaskRunner(TaskDefinition<V> definition, AtomicReference<Map<?, ?>> state,
                   TaskLifecycleListener lifecycleListener) {
            this.original = definition.getCommand();
            this.taskName = definition.getName();
            this.lifecycleListener = lifecycleListener;
            this.state = state;
            init();
        }

        private void init() {
            Map snapshot = state.get();
            if (original instanceof StatefulTask && !snapshot.isEmpty()) {
                ((StatefulTask) original).load(snapshot);
            }
        }

        @Override
        public V call()
                throws Exception {
            beforeRun();
            try {
                return original.call();
            } finally {
                afterRun();
            }
        }

        @Override
        public void run() {
            try {
                call();
            } catch (Exception e) {
                sneakyThrow(e);
            }
        }

        private void beforeRun() {
            if (logger.isFinestEnabled()) {
                logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "][Task: " + taskName + "] "
                        + "Entering running mode.");
            }

            lifecycleListener.onBeforeRun();
        }

        private void afterRun() {
            try {
                lifecycleListener.onAfterRun();
                if (original instanceof StatefulTask) {
                    Map snapshot = new HashMap();
                    ((StatefulTask) original).save(snapshot);

                    Map readOnlySnapshot = Collections.unmodifiableMap(snapshot);
                    state.set(readOnlySnapshot);
                    publishStateToReplicas(taskName, readOnlySnapshot);
                }
            } finally {
                notifyResultReady();
            }

            if (logger.isFinestEnabled()) {
                logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "][Task: " + taskName + "] "
                        + "Exiting running mode.");
            }
        }

        private void notifyResultReady() {
            Operation op = new ResultReadyNotifyOperation(offprintHandler(taskName));
            createInvocationBuilder(op)
                    .setCallTimeout(Long.MAX_VALUE)
                    .invoke();
        }

    }


}
