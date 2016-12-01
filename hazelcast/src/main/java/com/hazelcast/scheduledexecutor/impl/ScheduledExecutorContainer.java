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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService.SERVICE_NAME;
import static com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService.fullyQualifiedDurableExecutorName;
import static com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor.Mode.IDLE;
import static com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor.Mode.RUNNING;
import static com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor.Mode.SAFEPOINT;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;

public class ScheduledExecutorContainer {

    private final ILogger logger;

    private final String name;

    private final String fullyQualifiedDurableName;

    private final NodeEngine nodeEngine;

    private final InternalExecutionService executionService;

    private final int partitionId;

    private final int durability;

    private final ConcurrentMap<String, ScheduledTaskDescriptor> tasks;

    ScheduledExecutorContainer(String name, int partitionId, NodeEngine nodeEngine,
                                      int durability) {
        this(name, partitionId, nodeEngine, durability, new ConcurrentHashMap<String, ScheduledTaskDescriptor>());
    }

    ScheduledExecutorContainer(String name, int partitionId, NodeEngine nodeEngine,
                                      int durability, ConcurrentMap<String, ScheduledTaskDescriptor> tasks) {
        this.logger = nodeEngine.getLogger(getClass());
        this.name = name;
        this.fullyQualifiedDurableName = fullyQualifiedDurableExecutorName(name);
        this.nodeEngine = nodeEngine;
        this.executionService = (InternalExecutionService) nodeEngine.getExecutionService();
        this.partitionId = partitionId;
        this.durability = durability;
        this.tasks = tasks;
    }

    public <V> ScheduledFuture<V> schedule(TaskDefinition definition) {
        checkNotDuplicateTask(definition.getName());
        return createContextAndSchedule(definition);
    }

    public boolean cancel(String taskName, boolean mayInterruptIfRunning) {
        checkNotStaleTask(taskName);

        if (logger.isFinestEnabled()) {
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

    public void dispose(String taskName) {
        checkNotStaleTask(taskName);

        if (logger.isFinestEnabled()) {
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "] Disposing " + taskName);
        }

        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        ScheduledFuture scheduledFuture = descriptor.getScheduledFuture();
        if (!scheduledFuture.isDone()) {
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
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "] Stash size: " + tasks.size());
        }
    }

    public void unstash(String taskName) {
        tasks.remove(taskName);
    }

    public Collection<ScheduledTaskDescriptor> getTasks() {
        return tasks.values();
    }

    public void syncState(String taskName, Map newState) {
        tasks.get(taskName).setStateSnapshot(newState);
    }

    public boolean shouldParkGetResult(String taskName) {
        if (!tasks.containsKey(taskName)) {
            return false;
        }

        return tasks.get(taskName).getScheduledFuture() == null || !tasks.get(taskName).getScheduledFuture().isDone();
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

    <V> ScheduledFuture<V> createContextAndSchedule(TaskDefinition definition) {
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

        return (ScheduledFuture<V>) descriptor.getScheduledFuture();
    }

    void promoteStash() {
        for (ScheduledTaskDescriptor descriptor : tasks.values()) {
            if (descriptor.getScheduledFuture() == null) {
                doSchedule(descriptor);
            }
        }
    }

    Map<String, ScheduledTaskDescriptor> prepareForReplication(boolean migrationMode) {
        Map<String, ScheduledTaskDescriptor> replicas = new HashMap<String, ScheduledTaskDescriptor>();

        for (ScheduledTaskDescriptor descriptor : tasks.values()) {
            try {
                // Block until the task (running in Executor thread) is in a safe point - no progress -
                // This way we guarantee ordering between tasks execution and migrations without having to
                // run the tasks in the partitions. If the task migrates successfully we cancel execution
                // here to avoid having a period of time where this task can publish yet another state.
                // If this is not a migration, the task will resume.
                while (!descriptor.getModeRef().compareAndSet(IDLE, SAFEPOINT)) {
                    Thread.yield();
                }

                ScheduledTaskDescriptor replica = new ScheduledTaskDescriptor(
                        descriptor.getDefinition(),
                        descriptor.getStateSnapshot(),
                        descriptor.getStats());
                replicas.put(descriptor.getDefinition().getName(), replica);
            } finally {
                if (migrationMode) {
                    descriptor.getScheduledFuture().cancel(true);
                    // Nullify record so we can re-schedule in the event of rollback
                    descriptor.setScheduledFuture(null);
                }

                descriptor.getModeRef().set(IDLE);
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
     * State is synced after every run with all replicas.
     * When replicas get promoted, they start of, with the latest state see {@link TaskRunner#init()}
     */
    protected void publishStateToReplicas(String taskName, Map snapshot) {
        int maxAllowedBackupCount = nodeEngine.getPartitionService().getMaxAllowedBackupCount();
        boolean replicationNotRequired = Math.min(maxAllowedBackupCount, getDurability()) == 0;
        if (replicationNotRequired) {
            return;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("[Scheduler: " + name + "][Partition: " + partitionId + "][Task: " + taskName + "] "
                    + "Publishing state, to replicas. State: " + snapshot);
        }

        Operation op = new SyncStateOperation(getName(), taskName, snapshot);
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

        ScheduledFuture<?> future;
        TaskRunner<V> runner;
        switch (definition.getType()) {
            case SINGLE_RUN:
                runner = new TaskRunner<V>(descriptor);
                future = executionService.scheduleDurable(fullyQualifiedDurableName, (Callable) runner,
                        definition.getInitialDelay(), definition.getUnit());
                break;
            case WITH_REPETITION:
                runner = new TaskRunner<V>(descriptor);
                future = executionService.scheduleDurableWithRepetition(fullyQualifiedDurableName,
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

        private final TaskLifecycleListener lifecycleListener;

        TaskRunner(ScheduledTaskDescriptor descriptor) {
            this.descriptor = descriptor;
            this.original = descriptor.getDefinition().getCommand();
            this.taskName = descriptor.getDefinition().getName();
            this.lifecycleListener = descriptor.getStats();
            init();
        }

        private void init() {
            Map snapshot = descriptor.getStateSnapshot();
            if (original instanceof StatefulTask && !snapshot.isEmpty()) {
                ((StatefulTask) original).load(snapshot);
            }
        }

        @Override
        public V call()
                throws Exception {
            try {
                while (!descriptor.getModeRef().compareAndSet(IDLE, RUNNING)) {
                    Thread.yield();
                }

                return call0();
            } finally {
                descriptor.getModeRef().set(IDLE);
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

        private V call0() throws Exception {
            beforeRun();
            try {
                return original.call();
            } finally {
                afterRun();
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
                    descriptor.setStateSnapshot(readOnlySnapshot);
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
