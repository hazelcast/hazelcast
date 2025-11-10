/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.ExecutorStats;
import com.hazelcast.scheduledexecutor.DuplicateTaskException;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.ScheduledTaskStatistics;
import com.hazelcast.scheduledexecutor.StaleTaskException;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor.Status;
import com.hazelcast.scheduledexecutor.impl.operations.SyncStateOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.merge.ScheduledExecutorMergingEntryImpl;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.ScheduledExecutorMergeTypes;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService.SERVICE_NAME;
import static com.hazelcast.scheduledexecutor.impl.TaskDefinition.Type.AT_FIXED_RATE;
import static com.hazelcast.scheduledexecutor.impl.TaskDefinition.Type.SINGLE_RUN;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static java.lang.String.format;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

@SuppressWarnings("checkstyle:methodcount")
public class ScheduledExecutorContainer {

    protected final ConcurrentMap<String, ScheduledTaskDescriptor> tasks;

    private final boolean statisticsEnabled;
    private final int durability;
    private final int partitionId;

    private final String name;
    private final ILogger logger;
    private final NodeEngine nodeEngine;
    private final ExecutionService executionService;
    /**
     * Permits are acquired through two different places
     * a. When a task is scheduled by the user-facing API
     * i.e. {@link IScheduledExecutorService#schedule(Runnable, long, TimeUnit)}
     * whereas the permit policy is enforced, rejecting new tasks once the capacity is reached.
     * b. When a task is promoted (i.e. migration finished)
     * whereas the permit policy is not-enforced, meaning that actual task count might be more than the configured capacity,
     * but that is purposefully done to prevent any data-loss during node/cluster failures.
     * <p>
     * Permits are released similarly through two different places
     * a. When a task is disposed by user-facing API
     * i.e. {@link IScheduledFuture#dispose()} or {@link IScheduledExecutorService#destroy()}
     * b. When a task is suspended (i.e. migration started / roll-backed)
     * Note: Permit releases are done, only if the task was previously active
     * (i.e. {@link ScheduledTaskDescriptor#status} == {@link Status#ACTIVE}}
     *
     * As a result, {@link #tasks} size will be inconsistent with the number of acquired permits at times.
     */
    private final CapacityPermit permit;
    private final ExecutorStats executorStats;
    private final @Nullable String userCodeNamespace;

    ScheduledExecutorContainer(String name, int partitionId, NodeEngine nodeEngine, CapacityPermit permit,
                               int durability, boolean statisticsEnabled, @Nullable String userCodeNamespace) {
        this(name, partitionId, nodeEngine, permit, durability, new ConcurrentHashMap<>(), statisticsEnabled, userCodeNamespace);
    }

    ScheduledExecutorContainer(String name, int partitionId,
                               NodeEngine nodeEngine,
                               CapacityPermit permit, int durability,
                               ConcurrentMap<String, ScheduledTaskDescriptor> tasks,
                               boolean statisticsEnabled,
                               @Nullable String userCodeNamespace) {
        this.logger = nodeEngine.getLogger(getClass());
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.executionService = nodeEngine.getExecutionService();
        this.partitionId = partitionId;
        this.durability = durability;
        this.permit = permit;
        this.tasks = tasks;
        this.statisticsEnabled = statisticsEnabled;
        DistributedScheduledExecutorService service = nodeEngine.getService(SERVICE_NAME);
        this.executorStats = service.getExecutorStats();
        this.userCodeNamespace = userCodeNamespace;
    }

    public ExecutorStats getExecutorStats() {
        return executorStats;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public ScheduledFuture schedule(TaskDefinition definition) {
        checkNotDuplicateTask(definition.getName());
        acquirePermit(false);
        return createContextAndSchedule(definition);
    }

    public boolean cancel(String taskName) {
        checkNotStaleTask(taskName);
        log(FINEST, taskName, "Canceling");
        boolean cancelled = tasks.get(taskName).cancel(true);
        if (statisticsEnabled && cancelled) {
            executorStats.cancelExecution(name);
        }
        return cancelled;
    }

    public boolean has(String taskName) {
        return tasks.containsKey(taskName);
    }

    public Object get(String taskName)
            throws ExecutionException, InterruptedException {
        checkNotStaleTask(taskName);
        return tasks.get(taskName).get();
    }

    public long getDelay(String taskName, TimeUnit unit) {
        checkNotStaleTask(taskName);
        return tasks.get(taskName).getDelay(unit);
    }

    public ScheduledTaskStatistics getStatistics(String taskName) {
        checkNotStaleTask(taskName);
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        return descriptor.getStatsSnapshot();
    }

    public boolean isCancelled(String taskName) {
        checkNotStaleTask(taskName);
        return tasks.get(taskName).isCancelled();
    }

    public boolean isDone(String taskName) {
        checkNotStaleTask(taskName);
        return tasks.get(taskName).isDone();
    }

    public void destroy() {
        log(FINEST, "Destroying container...");

        for (String task : tasks.keySet()) {
            try {
                dispose(task);
            } catch (Exception ex) {
                log(WARNING, task, "Error while destroying", ex);
            }
        }
    }

    public void dispose(String taskName) {
        checkNotStaleTask(taskName);
        log(FINEST, taskName, "Disposing");

        ScheduledTaskDescriptor descriptor = tasks.remove(taskName);
        if (descriptor.isActive()) {
            releasePermit();
        }

        descriptor.cancel(true);
    }

    public void enqueueSuspended(TaskDefinition definition) {
        enqueueSuspended(new ScheduledTaskDescriptor(definition), false);
    }

    public void enqueueSuspended(ScheduledTaskDescriptor descriptor, boolean force) {
        if (logger.isFinestEnabled()) {
            log(FINEST, "Enqueuing suspended, i.e., backup: " + descriptor.getDefinition());
        }

        boolean keyExists = tasks.containsKey(descriptor.getDefinition().getName());
        if (force || !keyExists) {
            tasks.put(descriptor.getDefinition().getName(), descriptor);
        }
    }

    public Collection<ScheduledTaskDescriptor> getTasks() {
        return tasks.values();
    }

    public void syncState(String taskName, Map newState, ScheduledTaskStatisticsImpl stats, ScheduledTaskResult resolution) {
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        if (descriptor == null) {
            log(FINEST, taskName, "Sync state attempt on a defunct descriptor");
            return;
        }

        if (logger.isFinestEnabled()) {
            log(FINEST, taskName, "New state received " + newState);
        }

        descriptor.setState(newState);
        descriptor.setStats(stats);

        if (descriptor.getTaskResult() != null) {
            // Task result previously populated - i.e. through cancel
            // Ignore all subsequent results
            if (logger.isFineEnabled()) {
                log(FINE, taskName, format("New state ignored! Current: %s New: %s ", descriptor.getTaskResult(), resolution));
            }
        } else {
            descriptor.setTaskResult(resolution);
        }

        if (descriptor.getDefinition().isAutoDisposable() && descriptor.isDone()) {
            dispose(taskName);
        }
    }

    public boolean shouldParkGetResult(String taskName) {
        return tasks.containsKey(taskName) && (tasks.get(taskName).getTaskResult() == null || !isDone(taskName));
    }

    public int getDurability() {
        return durability;
    }

    public String getName() {
        return name;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Nullable
    public String getUserCodeNamespace() {
        return userCodeNamespace;
    }

    public ScheduledTaskHandler offprintHandler(String taskName) {
        return ScheduledTaskHandlerImpl.of(partitionId, getName(), taskName);
    }

    /**
     * Attempts to promote and schedule all suspended tasks on this partition.
     * Exceptions throw during rescheduling will be rethrown and will prevent
     * further suspended tasks from being scheduled.
     */
    public void promoteSuspended() {
        for (ScheduledTaskDescriptor descriptor : tasks.values()) {
            try {
                log(FINEST, descriptor.getDefinition().getName(), "Attempting promotion");
                boolean wasActive = descriptor.isActive();
                if (descriptor.canBeScheduled()) {
                    doSchedule(descriptor);
                }

                if (!wasActive) {
                    acquirePermit(true);
                }

                descriptor.setActive();
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
    }

    /**
     * Merges the given {@link ScheduledExecutorMergeTypes} via the given {@link SplitBrainMergePolicy}.
     *
     * @param mergingEntry the {@link ScheduledExecutorMergeTypes} instance to merge
     * @param mergePolicy  the {@link SplitBrainMergePolicy} instance to apply
     * @return the used {@link ScheduledTaskDescriptor} if merge is applied, otherwise {@code null}
     */
    public ScheduledTaskDescriptor merge(
            ScheduledExecutorMergeTypes mergingEntry,
            SplitBrainMergePolicy<ScheduledTaskDescriptor, ScheduledExecutorMergeTypes, ScheduledTaskDescriptor> mergePolicy) {
        SerializationService serializationService = nodeEngine.getSerializationService();
        mergingEntry = (ScheduledExecutorMergeTypes) serializationService.getManagedContext().initialize(mergingEntry);
        mergePolicy = (SplitBrainMergePolicy<ScheduledTaskDescriptor, ScheduledExecutorMergeTypes, ScheduledTaskDescriptor>)
                serializationService.getManagedContext().initialize(mergePolicy);

        // try to find an existing task with the same definition
        ScheduledTaskDescriptor mergingTask = ((ScheduledExecutorMergingEntryImpl) mergingEntry).getRawValue();
        ScheduledTaskDescriptor existingTask = null;
        for (ScheduledTaskDescriptor task : tasks.values()) {
            if (mergingTask.equals(task)) {
                existingTask = task;
                break;
            }
        }
        if (existingTask == null) {
            ScheduledTaskDescriptor newTask = mergePolicy.merge(mergingEntry, null);
            if (newTask != null) {
                enqueueSuspended(newTask, false);
                return newTask;
            }
        } else {
            ScheduledExecutorMergeTypes existingEntry = createMergingEntry(serializationService, existingTask);
            ScheduledTaskDescriptor newTask = mergePolicy.merge(mergingEntry, existingEntry);
            // we are using == instead of equals() for the task comparison,
            // since the descriptor may have the same fields for merging and existing entry,
            // but we still want to be able to choose which one is merged (e.g. PassThroughMergePolicy)
            if (newTask != null && newTask != existingTask) {
                // cancel the existing task, before replacing it
                existingTask.cancel(true);
                enqueueSuspended(newTask, true);
                return newTask;
            }
        }
        // the merging task was already suspended on the original node, so we don't have to cancel it here
        return null;
    }

    private void releasePermit() {
        permit.release();
    }

    private void acquirePermit(boolean quietly) {
        if (quietly) {
            permit.acquireQuietly();
        } else {
            permit.acquire();
        }
    }

    ScheduledFuture createContextAndSchedule(TaskDefinition definition) {
        if (logger.isFinestEnabled()) {
            log(FINEST, "Creating new task context for " + definition);
        }

        ScheduledTaskDescriptor descriptor = new ScheduledTaskDescriptor(definition);
        if (tasks.putIfAbsent(definition.getName(), descriptor) == null) {
            doSchedule(descriptor);
        }

        if (logger.isFinestEnabled()) {
            log(FINEST, "Queue size: " + tasks.size());
        }

        return descriptor.getScheduledFuture();
    }

    /**
     * Returns all task descriptors on this container, mapped by task name.
     *
     * @return a map of all tasks on this container
     */
    Map<String, ScheduledTaskDescriptor> prepareForReplication() {
        Map<String, ScheduledTaskDescriptor> replicas = createHashMap(tasks.size());
        for (ScheduledTaskDescriptor descriptor : tasks.values()) {
            try {
                ScheduledTaskDescriptor replica = new ScheduledTaskDescriptor(descriptor.getDefinition(),
                        descriptor.getState(),
                        descriptor.getStatsSnapshot(),
                        descriptor.getTaskResult());
                replicas.put(descriptor.getDefinition().getName(), replica);
            } catch (Exception ex) {
                sneakyThrow(ex);
            }
        }
        return replicas;
    }

    /**
     * Attempts to cancel and interrupt all tasks on this container. Exceptions
     * thrown during task cancellation will be rethrown and prevent further
     * tasks from being cancelled.
     */
    void suspendTasks() {
        for (ScheduledTaskDescriptor descriptor : tasks.values()) {
            // Best effort to cancel & interrupt the task.
            // In the case of Runnable the DelegateAndSkipOnConcurrentExecutionDecorator is not exposing access
            // to the Executor's Future, hence, we have no access on the runner thread to interrupt. In this case
            // the line below is only cancelling future runs.
            try {
                if (descriptor.suspend()) {
                    releasePermit();
                }
                if (logger.isFinestEnabled()) {
                    log(FINEST, descriptor.getDefinition().getName(), "Suspended");
                }
            } catch (Exception ex) {
                throw rethrow(ex);
            }
        }
    }

    void checkNotDuplicateTask(String taskName) {
        if (tasks.containsKey(taskName)) {
            throw new DuplicateTaskException(
                    "There is already a task " + "with the same name '" + taskName + "' in '" + getName() + "'");
        }
    }

    /**
     * State is published after every run.
     * When replicas get promoted, they start with the latest state.
     */
    void publishTaskState(String taskName, Map stateSnapshot, ScheduledTaskStatisticsImpl statsSnapshot,
                          ScheduledTaskResult result) {
        if (logger.isFinestEnabled()) {
            log(FINEST, "Publishing state, to replicas. State: " + stateSnapshot);
        }

        Operation op = new SyncStateOperation(getName(), taskName, stateSnapshot, statsSnapshot, result);
        createInvocationBuilder(op)
                .invoke()
                .joinInternal();
    }

    protected InvocationBuilder createInvocationBuilder(Operation op) {
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(SERVICE_NAME, op, partitionId);
    }

    protected void log(Level level, String msg) {
        log(level, null, msg);
    }

    protected void log(Level level, String taskName, String msg) {
        log(level, taskName, msg, null);
    }

    protected void log(Level level, String taskName, String msg, Throwable t) {
        if (logger.isLoggable(level)) {
            StringBuilder log = new StringBuilder();
            log.append("[Scheduler: " + name + "][Partition: " + partitionId + "]");
            if (taskName != null) {
                log.append("[Task: " + taskName + "] ");
            }
            log.append(msg);
            logger.log(level, log.toString(), t);
        }
    }

    private <V> void doSchedule(ScheduledTaskDescriptor descriptor) {
        assert descriptor.getScheduledFuture() == null;
        TaskDefinition definition = descriptor.getDefinition();

        ScheduledFuture future;
        TaskRunner<V> runner;
        try {
            switch (definition.getType()) {
                case SINGLE_RUN:
                    runner = new TaskRunner<>(this, descriptor, SINGLE_RUN);
                    future = new DelegatingScheduledFutureStripper<V>(executionService
                            .scheduleDurable(name, (Callable) runner, definition.getInitialDelay(), definition.getUnit()));
                    break;
                case AT_FIXED_RATE:
                    runner = new TaskRunner<>(this, descriptor, AT_FIXED_RATE);
                    future = executionService
                            .scheduleDurableWithRepetition(name, runner, definition.getInitialDelay(), definition.getPeriod(),
                                    definition.getUnit());
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        } catch (RejectedExecutionException e) {
            if (statisticsEnabled) {
                getExecutorStats().rejectExecution(name);
            }
            throw e;
        }

        descriptor.setScheduledFuture(future);

        if (logger.isFinestEnabled()) {
            log(FINEST, definition.getName(), "Scheduled");
        }
    }

    private void checkNotStaleTask(String taskName) {
        if (!has(taskName)) {
            throw new StaleTaskException("Task with name " + taskName + " not found. ");
        }
    }
}
