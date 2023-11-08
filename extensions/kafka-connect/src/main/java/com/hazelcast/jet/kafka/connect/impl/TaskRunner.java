/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kafka.connect.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class TaskRunner {
    private ILogger logger = Logger.getLogger(TaskRunner.class);
    private final String name;
    private final ReentrantLock taskLifecycleLock = new ReentrantLock();
    private final State state;
    private final SourceTaskFactory sourceTaskFactory;
    private volatile boolean running;
    private volatile boolean reconfigurationNeeded;
    private SourceTask task;
    private final AtomicReference<Map<String, String>> taskConfigReference = new AtomicReference<>();

    TaskRunner(String name, State state, SourceTaskFactory sourceTaskFactory) {
        this.name = name;
        this.state = state;
        this.sourceTaskFactory = sourceTaskFactory;
    }

    public void setLogger(ILogger logger) {
        this.logger = logger;
    }

    public List<SourceRecord> poll() {
        restartTaskIfNeeded();
        if (running) {
            return doPoll();
        } else {
            return Collections.emptyList();
        }
    }

    public void stop() {
        try {
            taskLifecycleLock.lock();
            if (running) {
                logger.fine("Stopping task '" + name + "'");
                task.stop();
                logger.fine("Task '" + name + "' stopped");
            }
        } finally {
            running = false;
            taskLifecycleLock.unlock();
        }
    }

    private List<SourceRecord> doPoll() {
        try {
            List<SourceRecord> sourceRecords = task.poll();
            return sourceRecords == null ? Collections.emptyList() : sourceRecords;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw rethrow(e);
        }
    }

    private void restartTaskIfNeeded() {
        if (reconfigurationNeeded) {
            reconfigurationNeeded = false;
            try {
                stop();
            } catch (Exception ex) {
                logger.warning("Stopping task '" + name + "' failed but proceeding with re-start", ex);
            }
        }
        start();
    }

    public void updateTaskConfig(Map<String, String> taskConfig) {
        try {
            taskLifecycleLock.lock();
            if (!Objects.equals(this.taskConfigReference.get(), taskConfig)) {
                logger.info("Updating task '" + name + "' configuration");
                taskConfigReference.set(taskConfig);
                reconfigurationNeeded = true;
            }
        } finally {
            taskLifecycleLock.unlock();
        }
    }

    private void start() {
        try {
            taskLifecycleLock.lock();
            if (!running) {
                if (taskConfigReference.get() != null) {
                    SourceTask taskLocal = sourceTaskFactory.create();
                    logger.info("Initializing task '" + name + "'");
                    taskLocal.initialize(new JetSourceTaskContext(taskConfigReference.get(), state));
                    logger.info("Starting task '" + name + "'");
                    taskLocal.start(taskConfigReference.get());
                    this.task = taskLocal;
                    running = true;
                } else {
                    logger.finest("No task config for task '" + name + "'");
                }
            }
        } finally {
            taskLifecycleLock.unlock();
        }
    }

    public void commit() {
        if (running) {
            try {
                task.commit();
            } catch (InterruptedException e) {
                logger.warning("Interrupted while committing");
                Thread.currentThread().interrupt();
            }
        }
    }

    public void commitRecord(SourceRecord rec) {
        state.commitRecord(rec);
        try {
            if (running) {
                task.commitRecord(rec, null);
            }
        } catch (InterruptedException ie) {
            logger.warning("Interrupted while committing record");
            Thread.currentThread().interrupt();
        }
    }

    public State getSnapshotCopy() {
        State snapshot = new State();
        snapshot.load(state);
        return snapshot;
    }

    public void restoreSnapshot(State state) {
        this.state.load(state);
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "TaskRunner{" +
               "name='" + name + '\'' +
               '}';
    }

    @FunctionalInterface
    interface SourceTaskFactory {
        SourceTask create();
    }

}
