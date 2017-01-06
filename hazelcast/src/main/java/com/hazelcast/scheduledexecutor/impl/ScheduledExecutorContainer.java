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
import com.hazelcast.util.MapUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;

public class ScheduledExecutorContainer {

    protected final ConcurrentMap<String, ScheduledTaskDescriptor> tasks;

    private final ILogger logger;

    private final String name;

    private final NodeEngine nodeEngine;

    private final InternalExecutionService executionService;

    private final int partitionId;

    private final int durability;

    ScheduledExecutorContainer(String name, int partitionId, NodeEngine nodeEngine,
                                      int durability) {
        this(name, partitionId, nodeEngine, durability, new ConcurrentHashMap<String, ScheduledTaskDescriptor>());
    }

    ScheduledExecutorContainer(String name, int partitionId, NodeEngine nodeEngine,
                                      int durability, ConcurrentMap<String, ScheduledTaskDescriptor> tasks) {
        this.logger = nodeEngine.getLogger(getClass());
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.executionService = (InternalExecutionService) nodeEngine.getExecutionService();
        this.partitionId = partitionId;
        this.durability = durability;
        this.tasks = tasks;
    }

    public ScheduledFuture schedule(TaskDefinition definition) {
        checkNotDuplicateTask(definition.getName());
        return createContextAndSchedule(definition);
    }

    public boolean cancel(String taskName)
            throws ExecutionException, InterruptedException {
        checkNotStaleTask(taskName);

        if (logger.isFinestEnabled()) {
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "] Cancelling " + taskName);
        }

        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        boolean cancelled = descriptor.getScheduledFuture().cancel(true);

        if (TaskDefinition.Type.SINGLE_RUN.equals(descriptor.getDefinition().getType())) {
            // Cancel the delegating Executor's Future (in case of Callable, see. DelegatingCallableTaskDecorator)
            cancelled = ((Future) descriptor.getScheduledFuture().get()).cancel(true);
        }

        return cancelled;
    }

    public Object get(String taskName)
            throws ExecutionException, InterruptedException {
        checkNotStaleTask(taskName);
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        if (TaskDefinition.Type.SINGLE_RUN.equals(descriptor.getDefinition().getType())) {
            // Get the result from the delegating's Executor Future (in case of Callable, see. DelegatingCallableTaskDecorator)
            return ((Future) descriptor.getScheduledFuture().get()).get();
        } else {
            return descriptor.getScheduledFuture().get();
        }
    }

    public long getDelay(String taskName, TimeUnit unit) {
        checkNotStaleTask(taskName);
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        return descriptor.getScheduledFuture().getDelay(unit);
    }

    public ScheduledTaskStatistics getStatistics(String taskName) {
        checkNotStaleTask(taskName);
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        return descriptor.getStatsSnapshot();
    }

    public boolean isCancelled(String taskName)
            throws ExecutionException, InterruptedException {
        checkNotStaleTask(taskName);
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        if (TaskDefinition.Type.SINGLE_RUN.equals(descriptor.getDefinition().getType())) {
            // Check the status from the delegating Executor Future (in case of Callable, see. DelegatingCallableTaskDecorator)
            return ((Future) descriptor.getScheduledFuture().get()).isCancelled();
        } else {
            return descriptor.getScheduledFuture().isCancelled();
        }
    }

    public boolean isDone(String taskName)
            throws ExecutionException, InterruptedException {
        checkNotStaleTask(taskName);
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        if (TaskDefinition.Type.SINGLE_RUN.equals(descriptor.getDefinition().getType())) {
            // Check the status from the delegating Executor Future (in case of Callable, see. DelegatingCallableTaskDecorator)
            return ((Future) descriptor.getScheduledFuture().get()).isDone();
        } else {
            return descriptor.getScheduledFuture().isDone();
        }
    }

    public void destroy() {
        if (logger.isFinestEnabled()) {
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "] Destroying container...");
        }

        for (ScheduledTaskDescriptor descriptor : tasks.values()) {
            if (descriptor.getScheduledFuture() != null) {
                try {
                    descriptor.getScheduledFuture().cancel(true);
                } catch (Exception ex) {
                    logger.warning("[Scheduler: " + name + "][Partition: " + partitionId + "] "
                            + "Destroying " + descriptor.getDefinition().getName() + " error: ", ex);
                }
            }
        }
    }

    public void dispose(String taskName)
            throws ExecutionException, InterruptedException {
        checkNotStaleTask(taskName);

        if (logger.isFinestEnabled()) {
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "] Disposing " + taskName);
        }

        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        Future scheduledFuture = descriptor.getScheduledFuture();
        if (!scheduledFuture.isDone()) {
            if (TaskDefinition.Type.SINGLE_RUN.equals(descriptor.getDefinition().getType())) {
                // Cancel the delegating Executor Future (in case of Callable, see. DelegatingCallableTaskDecorator)
                ((Future) scheduledFuture.get()).cancel(true);
            }

            // Cancel the ScheduledExecutor ScheduledFuture
            scheduledFuture.cancel(true);
        }

        tasks.remove(taskName);
    }

    public void stash(TaskDefinition definition) {
        if (logger.isFinestEnabled()) {
            logger.finest("[Backup Scheduler: " + name + "][Partition: " + partitionId + "] Stashing " + definition);
        }

        if (!tasks.containsKey(definition.getName())) {
            ScheduledTaskDescriptor descriptor = new ScheduledTaskDescriptor(definition);
            tasks.put(definition.getName(), descriptor);
        }

        if (logger.isFinestEnabled()) {
            logger.finest("[Backup Scheduler: " + name + "][Partition: " + partitionId + "] Stash size: " + tasks.size());
        }
    }

    public void unstash(String taskName) {
        tasks.remove(taskName);
    }

    public Collection<ScheduledTaskDescriptor> getTasks() {
        return tasks.values();
    }

    public void syncState(String taskName, Map newState, ScheduledTaskStatisticsImpl stats)
            throws ExecutionException, InterruptedException {
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        if (descriptor == null) {
            // Task previously disposed or unstashed
            if (logger.isFinestEnabled()) {
                logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "] syncState attempt "
                        + "but no descriptor found for task: " + taskName);
            }
            return;
        }

        descriptor.setState(newState);
        descriptor.setStats(stats);
    }

    public boolean shouldParkGetResult(String taskName)
            throws ExecutionException, InterruptedException {
        if (!tasks.containsKey(taskName)) {
            return false;
        }

        return tasks.get(taskName).getScheduledFuture() == null || !isDone(taskName);
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

    ScheduledFuture createContextAndSchedule(TaskDefinition definition) {
        if (logger.isFinestEnabled()) {
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "] Scheduling " + definition);
        }

        ScheduledTaskDescriptor descriptor = new ScheduledTaskDescriptor(definition);
        if (tasks.putIfAbsent(definition.getName(), descriptor) == null) {
            doSchedule(descriptor);
        }

        if (logger.isFinestEnabled()) {
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "] Queue size: " + tasks.size());
        }

        return descriptor.getScheduledFuture();
    }

    void promoteStash() {
        for (ScheduledTaskDescriptor descriptor : tasks.values()) {
            if (descriptor.getScheduledFuture() == null) {
                doSchedule(descriptor);
            }
        }
    }

    Map<String, ScheduledTaskDescriptor> prepareForReplication(boolean migrationMode) {
        Map<String, ScheduledTaskDescriptor> replicas = MapUtil.createHashMap(tasks.size());

        for (ScheduledTaskDescriptor descriptor : tasks.values()) {
            try {
                ScheduledTaskDescriptor replica = new ScheduledTaskDescriptor(
                        descriptor.getDefinition(),
                        descriptor.getStateSnapshot(),
                        descriptor.getStatsSnapshot());
                replicas.put(descriptor.getDefinition().getName(), replica);
            } finally {
                if (migrationMode) {
                    // Best effort to cancel & interrupt the task.
                    // In the case of Runnable the DelegateAndSkipOnConcurrentExecutionDecorator is not exposing access
                    // to the Executor's Future, hence, we have no access on the runner thread to interrupt. In this case
                    // the line below is only cancelling future scheduling attempts.
                    descriptor.getScheduledFuture().cancel(true);
                    // Nullify record so we can re-schedule in the event of rollback
                    descriptor.setScheduledFuture(null);
                }
            }
        }

        return replicas;
    }

    void checkNotDuplicateTask(String taskName) {
        if (tasks.containsKey(taskName)) {
            throw new DuplicateTaskException("There is already a task "
                    + "with the same name '" + taskName + "' in '" + getName() + "'");
        }
    }

    /**
     * State is published after every run.
     * When replicas get promoted, they start of, with the latest state see {@link TaskRunner#initOnce()}
     */
    protected void publishTaskState(String taskName, Map stateSnapshot, ScheduledTaskStatisticsImpl statsSnapshot) {
        if (logger.isFinestEnabled()) {
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "][Task: " + taskName + "] "
                    + "Publishing state, to replicas. State: " + stateSnapshot);
        }

        Operation op = new SyncStateOperation(getName(), taskName, stateSnapshot, statsSnapshot);
        createInvocationBuilder(op)
                .invoke()
                .join();
    }

    protected InvocationBuilder createInvocationBuilder(Operation op) {
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(SERVICE_NAME, op, partitionId);
    }

    private <V> void doSchedule(ScheduledTaskDescriptor descriptor) {
        assert descriptor.getScheduledFuture() == null;
        TaskDefinition definition = descriptor.getDefinition();

        ScheduledFuture future;
        TaskRunner<V> runner;
        switch (definition.getType()) {
            case SINGLE_RUN:
                runner = new TaskRunner<V>(descriptor);
                    future = executionService.scheduleDurable(name, (Callable) runner,
                            definition.getInitialDelay(), definition.getUnit());
                break;
            case AT_FIXED_RATE:
                runner = new TaskRunner<V>(descriptor);
                future = executionService.scheduleDurableWithRepetition(name,
                        runner, definition.getInitialDelay(), definition.getPeriod(),
                        definition.getUnit());
                break;
            default:
                throw new IllegalArgumentException();
        }

        descriptor.setScheduledFuture(future);
    }

    private void checkNotStaleTask(String taskName) {
        if (!tasks.containsKey(taskName)) {
            throw new StaleTaskException("Task with name " + taskName + " not found. ");
        }
    }

    private class TaskRunner<V> implements Callable<V>, Runnable {

        private final String taskName;

        private final Callable<V> original;

        private final ScheduledTaskDescriptor descriptor;

        private final ScheduledTaskStatisticsImpl statistics;

        private boolean initted;

        TaskRunner(ScheduledTaskDescriptor descriptor) {
            this.descriptor = descriptor;
            this.original = descriptor.getDefinition().getCommand();
            this.taskName = descriptor.getDefinition().getName();
            this.statistics = descriptor.getStatsSnapshot();
            statistics.onInit();
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

        private void initOnce() {
            if (initted) {
                return;
            }

            Map snapshot = descriptor.getStateSnapshot();
            if (original instanceof StatefulTask && !snapshot.isEmpty()) {
                ((StatefulTask) original).load(snapshot);
            }

            initted = true;
        }

        private void beforeRun() {
            if (logger.isFinestEnabled()) {
                logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "][Task: " + taskName + "] "
                        + "Entering running mode.");
            }

            try {
                initOnce();
                statistics.onBeforeRun();
            } catch (Exception ex) {
                logger.warning("[Scheduler: " + name + "][Partition: " + partitionId + "][Task: " + taskName + "] "
                        + "Unexpected exception during beforeRun occurred: ", ex);
            }
        }

        private void afterRun() {
            try {
                statistics.onAfterRun();

                Map state = new HashMap();
                if (original instanceof StatefulTask) {
                    ((StatefulTask) original).save(state);
                }

                publishTaskState(taskName, state, statistics);
            } catch (Exception ex) {
                logger.warning("[Scheduler: " + name + "][Partition: " + partitionId + "][Task: " + taskName + "] "
                        + "Unexpected exception during afterRun occurred: ", ex);
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
