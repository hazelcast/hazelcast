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

package com.hazelcast.scheduleexecutor.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.scheduleexecutor.DuplicateTaskException;
import com.hazelcast.scheduleexecutor.ScheduledTaskStatistics;
import com.hazelcast.scheduleexecutor.StaleTaskException;
import com.hazelcast.scheduleexecutor.StatefulRunnable;
import com.hazelcast.scheduleexecutor.impl.operations.SyncStateOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.partition.IPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.scheduleexecutor.impl.DistributedScheduledExecutorService.SERVICE_NAME;

public class ScheduledExecutorContainer {

    private final String name;

    private final NodeEngine nodeEngine;

    private final InternalExecutionService executionService;

    private final int partitionId;

    private final int durability;

    private final Map<String, ScheduledTaskDescriptor> tasks;

    private final Map<String, BackupTaskDescriptor> backups;

    public ScheduledExecutorContainer(String name, int partitionId, NodeEngine nodeEngine,
                                      int durability) {
        this(name, partitionId, nodeEngine, durability, new HashMap<String, BackupTaskDescriptor>());
    }

    public ScheduledExecutorContainer(String name, int partitionId, NodeEngine nodeEngine,
                                      int durability, Map<String, BackupTaskDescriptor> backups) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.executionService = (InternalExecutionService) nodeEngine.getExecutionService();
        this.partitionId = partitionId;
        this.durability = durability;
        this.tasks = new HashMap<String, ScheduledTaskDescriptor>();
        this.backups = backups;
    }

    public <V> ScheduledFuture<?> schedule(RunnableDefinition definition) {
        checkNotDuplicateTask(definition.getName());

        AmendableScheduledTaskStatistics stats = new ScheduledTaskStatisticsImpl();
        Map taskState = new HashMap();

        ScheduledFuture<?> future = doSchedule(definition, taskState, stats);

        ScheduledTaskDescriptor descriptor = new ScheduledTaskDescriptor(definition, future, taskState, stats);
        tasks.put(definition.getName(), descriptor);
        return descriptor.getScheduledFuture();

    }

    private ScheduledFuture<?> doSchedule(RunnableDefinition definition,
                                          Map taskState, AmendableScheduledTaskStatistics stats) {

        Runnable taskAdapter = new RunnableTaskAdapter(definition, taskState, stats);

        ScheduledFuture<?> future = null;
        switch (definition.getType()) {
            case SINGLE_RUN:
                future = executionService.schedule(getName(),
                        taskAdapter, definition.getInitialDelay(), definition.getUnit());
                break;
            case WITH_REPETITION:
                future = executionService.scheduleWithRepetition(getName(),
                        taskAdapter, definition.getInitialDelay(), definition.getPeriod(),
                        definition.getUnit());
                break;
            default:
                throw new IllegalArgumentException();
        }

        return future;
    }

    public boolean cancel(String taskName, boolean mayInterruptIfRunning) {
        checkNotStaleTask(taskName);

        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        descriptor.getScheduledFuture().cancel(mayInterruptIfRunning);

        return descriptor.getScheduledFuture().isCancelled();
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

    public int compareTo(String taskName, Delayed o) {
        checkNotStaleTask(taskName);
        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        return descriptor.getScheduledFuture().compareTo(o);
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

        ScheduledTaskDescriptor descriptor = tasks.get(taskName);
        ScheduledFuture scheduledFuture = descriptor.getScheduledFuture();
        if (!scheduledFuture.isDone()) {
            scheduledFuture.cancel(true);
        }

        tasks.remove(taskName);
    }

    public void stash(RunnableDefinition definition) {
        // TODO tkountis - Consider whether the backup task should also be running or not
        // - provide use cases where either way is wrong
        if (!backups.containsKey(definition.getName())) {
            BackupTaskDescriptor descriptor = new BackupTaskDescriptor(definition);
            backups.put(definition.getName(), descriptor);
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

    public int getDurability() {
        return durability;
    }

    public String getName() {
        return name;
    }

    void promoteStash() {
        Iterator<Map.Entry<String, BackupTaskDescriptor>> backupEntries =
                backups.entrySet().iterator();

        while (backupEntries.hasNext()) {
            Map.Entry<String, BackupTaskDescriptor> entry = backupEntries.next();
            BackupTaskDescriptor descriptor = entry.getValue();
            doSchedule(descriptor.getDefinition(), descriptor.getMasterState(), descriptor.getMasterStats());
            backupEntries.remove();
        }

    }

    Map<String, BackupTaskDescriptor> prepareForReplication() {
        Map<String, BackupTaskDescriptor> replicas = new HashMap<String, BackupTaskDescriptor>();

        Iterator<Map.Entry<String, ScheduledTaskDescriptor>> taskEntries =
                tasks.entrySet().iterator();

        while (taskEntries.hasNext()) {
            Map.Entry<String, ScheduledTaskDescriptor> entry = taskEntries.next();
            ScheduledTaskDescriptor descriptor = entry.getValue();
            ScheduledFuture<?> future = descriptor.getScheduledFuture();

            // Stop primary one, so we can safely capture latest state.
            // However, may be incorrect state due to interruption.
            while (!future.isCancelled() && !future.isDone()) {
                future.cancel(true);
            }

            BackupTaskDescriptor replica = new BackupTaskDescriptor(descriptor.getDefinition());
            replica.setMasterState(descriptor.getState());
            replica.setMasterStats(descriptor.getStats());

            replicas.put(entry.getKey(), replica);
        }

        return replicas;
    }

    private void checkNotDuplicateTask(String taskName) {
        if (tasks.containsKey(taskName)) {
            throw new DuplicateTaskException("There is already a task "
                    + "with the same name '" + taskName + "' in '" + getName() + "'");
        }
    }

    private void checkNotStaleTask(String taskName) {
        if (!tasks.containsKey(taskName)) {
            throw new StaleTaskException("Task with name " + taskName + " not found.");
        }
    }

    class RunnableTaskAdapter implements Runnable {

        private final String taskName;

        private final Runnable original;

        private final Map state;

        private final AmendableScheduledTaskStatistics stats;

        RunnableTaskAdapter(RunnableDefinition definition, Map state, AmendableScheduledTaskStatistics stats) {
            this.original = definition.getRunnable();
            this.taskName = definition.getName();
            this.stats = stats;
            this.state = state;
        }

        @Override
        public void run() {
            beforeRun();
            try {
                original.run();
            } finally {
                afterRun();
            }
        }

        private void beforeRun() {
            stats.onBeforeRun();
            if (original instanceof StatefulRunnable) {
                //TODO tkountis - This needs to be done only for slave -> master promoted tasks, not all
                ((StatefulRunnable) original).loadState(state);
            }
        }

        private void afterRun() {
            stats.onAfterRun();
            if (original instanceof StatefulRunnable) {
                ((StatefulRunnable) original).saveState(state);
                publishStateToReplicas();
            }
        }

        private void publishStateToReplicas() {
            OperationService operationService = nodeEngine.getOperationService();
            IPartition partition = nodeEngine.getPartitionService().getPartition(partitionId);
            for (int i = 1; i < getDurability() + 1; i++ ) {
                Address address = partition.getReplicaAddress(i);
                Operation op = new SyncStateOperation(getName(), taskName, state);
                op.setPartitionId(partitionId);
                op.setReplicaIndex(i);

                operationService.createInvocationBuilder(SERVICE_NAME, op, address)
                                .setCallTimeout(Long.MAX_VALUE)
                                .invoke();
            }

        }

    }


}
