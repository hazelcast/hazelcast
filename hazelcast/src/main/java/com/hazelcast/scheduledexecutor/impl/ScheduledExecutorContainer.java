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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.scheduledexecutor.DuplicateTaskException;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.ScheduledTaskStatistics;
import com.hazelcast.scheduledexecutor.StaleTaskException;
import com.hazelcast.scheduledexecutor.impl.operations.SyncStateOperation;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.SplitBrainAwareDataContainer;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;

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

import static com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService.SERVICE_NAME;
import static com.hazelcast.spi.merge.SplitBrainEntryViews.createSplitBrainMergeEntryView;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.MapUtil.createHashMap;
import static java.lang.String.format;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

@SuppressWarnings("checkstyle:methodcount")
public class ScheduledExecutorContainer
        implements SplitBrainAwareDataContainer<String, ScheduledTaskDescriptor, ScheduledTaskDescriptor> {

    protected final ConcurrentMap<String, ScheduledTaskDescriptor> tasks;

    private final ILogger logger;

    private final String name;

    private final NodeEngine nodeEngine;

    private final InternalExecutionService executionService;

    private final int partitionId;

    private final int durability;

    private final int capacity;

    ScheduledExecutorContainer(String name, int partitionId, NodeEngine nodeEngine, int durability, int capacity) {
        this(name, partitionId, nodeEngine, durability, capacity, new ConcurrentHashMap<String, ScheduledTaskDescriptor>());
    }

    ScheduledExecutorContainer(String name, int partitionId, NodeEngine nodeEngine, int durability, int capacity,
                               ConcurrentMap<String, ScheduledTaskDescriptor> tasks) {
        this.logger = nodeEngine.getLogger(getClass());
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.executionService = (InternalExecutionService) nodeEngine.getExecutionService();
        this.partitionId = partitionId;
        this.durability = durability;
        this.capacity = capacity;
        this.tasks = tasks;
    }

    public ScheduledFuture schedule(TaskDefinition definition) {
        checkNotDuplicateTask(definition.getName());
        checkNotAtCapacity();
        return createContextAndSchedule(definition);
    }

    public boolean cancel(String taskName) {
        checkNotStaleTask(taskName);
        log(FINEST, taskName, "Canceling");
        return tasks.get(taskName).cancel(true);
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

        for (ScheduledTaskDescriptor descriptor : tasks.values()) {
            try {
                descriptor.cancel(true);
            } catch (Exception ex) {
                log(WARNING, descriptor.getDefinition().getName(), "Error while destroying", ex);
            }
        }
    }

    public void dispose(String taskName) {
        checkNotStaleTask(taskName);
        log(FINEST, taskName, "Disposing");

        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        descriptor.cancel(true);

        tasks.remove(taskName);
    }

    public void enqueueSuspended(TaskDefinition definition) {
        enqueueSuspended(new ScheduledTaskDescriptor(definition), false);
    }

    public void enqueueSuspended(ScheduledTaskDescriptor descriptor, boolean force) {
        if (logger.isFinestEnabled()) {
            log(FINEST, "Enqueuing suspended, i.e., backup: " + descriptor.getDefinition());
        }

        if (force || !tasks.containsKey(descriptor.getDefinition().getName())) {
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
            // Task result previously populated - ie. through cancel
            // Ignore all subsequent results
            if (logger.isFineEnabled()) {
                log(FINE, taskName, format("New state ignored! Current: %s New: %s ", descriptor.getTaskResult(), resolution));
            }
        } else {
            descriptor.setTaskResult(resolution);
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

    public ScheduledTaskHandler offprintHandler(String taskName) {
        return ScheduledTaskHandlerImpl.of(partitionId, getName(), taskName);
    }

    public void promoteSuspended() {
        for (ScheduledTaskDescriptor descriptor : tasks.values()) {
            try {
                log(FINEST, descriptor.getDefinition().getName(), "Attempting promotion");
                if (descriptor.shouldSchedule()) {
                    doSchedule(descriptor);
                }

            } catch (Exception e) {
                throw rethrow(e);
            }
        }
    }

    @Override
    public ScheduledTaskDescriptor merge(SplitBrainMergeEntryView<String, ScheduledTaskDescriptor> merging,
                                         SplitBrainMergePolicy mergePolicy) {
        nodeEngine.getSerializationService().getManagedContext().initialize(mergePolicy);

        // try to find an existing item with the same value
        ScheduledTaskDescriptor match = null;
        for (ScheduledTaskDescriptor item : tasks.values()) {
            if (merging.getValue().equals(item)) {
                match = item;
                break;
            }
        }

        ScheduledTaskDescriptor merged;
        if (match == null) {
            // Missing incoming entry
            merged = mergePolicy.merge(merging, null);
            if (merged != null) {
                enqueueSuspended(merged, false);
            }
        } else {
            // Found a match -> real merge
            SplitBrainMergeEntryView<String, ScheduledTaskDescriptor> matchEntryView = createSplitBrainMergeEntryView(match);
            merged = mergePolicy.merge(merging, matchEntryView);
            if (merged != null && !merged.equals(match)) {
                // Cancel matched one, before replacing it
                match.cancel(true);
                enqueueSuspended(merged, true);
            } else {
                merged = null;
            }
        }

        return merged;
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

    Map<String, ScheduledTaskDescriptor> prepareForReplication(boolean migrationMode) {

        Map<String, ScheduledTaskDescriptor> replicas = createHashMap(tasks.size());

        for (ScheduledTaskDescriptor descriptor : tasks.values()) {
            try {
                ScheduledTaskDescriptor replica = new ScheduledTaskDescriptor(descriptor.getDefinition(), descriptor.getState(),
                        descriptor.getStatsSnapshot(), descriptor.getTaskResult());
                replicas.put(descriptor.getDefinition().getName(), replica);
            } catch (Exception ex) {
                sneakyThrow(ex);
            } finally {
                if (migrationMode) {
                    // Best effort to cancel & interrupt the task.
                    // In the case of Runnable the DelegateAndSkipOnConcurrentExecutionDecorator is not exposing access
                    // to the Executor's Future, hence, we have no access on the runner thread to interrupt. In this case
                    // the line below is only cancelling future runs.
                    try {
                        descriptor.suspend();
                    } catch (Exception ex) {
                        throw rethrow(ex);
                    }
                }
            }
        }

        return replicas;
    }

    void checkNotDuplicateTask(String taskName) {
        if (tasks.containsKey(taskName)) {
            throw new DuplicateTaskException(
                    "There is already a task " + "with the same name '" + taskName + "' in '" + getName() + "'");
        }
    }

    void checkNotAtCapacity() {
        if (capacity != 0 && tasks.size() >= capacity) {
            throw new RejectedExecutionException(
                    "Maximum capacity (" + capacity + ") of tasks reached, " + "for scheduled executor (" + name + ")");
        }
    }

    /**
     * State is published after every run. When replicas get promoted, they start of, with the latest state see {@link
     * TaskRunner#initOnce()}
     */
    void publishTaskState(String taskName, Map stateSnapshot, ScheduledTaskStatisticsImpl statsSnapshot,
                          ScheduledTaskResult result) {
        if (logger.isFinestEnabled()) {
            log(FINEST, "Publishing state, to replicas. State: " + stateSnapshot);
        }

        Operation op = new SyncStateOperation(getName(), taskName, stateSnapshot, statsSnapshot, result);
        createInvocationBuilder(op).invoke().join();
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

        if (logger.isFinestEnabled()) {
            log(FINEST, definition.getName(), "Scheduled");
        }

        ScheduledFuture future;
        TaskRunner<V> runner;
        switch (definition.getType()) {
            case SINGLE_RUN:
                runner = new TaskRunner<V>(this, descriptor);
                future = new DelegatingScheduledFutureStripper<V>(executionService
                        .scheduleDurable(name, (Callable) runner, definition.getInitialDelay(), definition.getUnit()));
                break;
            case AT_FIXED_RATE:
                runner = new TaskRunner<V>(this, descriptor);
                future = executionService
                        .scheduleDurableWithRepetition(name, runner, definition.getInitialDelay(), definition.getPeriod(),
                                definition.getUnit());
                break;
            default:
                throw new IllegalArgumentException();
        }

        descriptor.setScheduledFuture(future);
    }

    private void checkNotStaleTask(String taskName) {
        if (!has(taskName)) {
            throw new StaleTaskException("Task with name " + taskName + " not found. ");
        }
    }

}
