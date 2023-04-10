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
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

class TaskRunner {
    private static final ILogger LOGGER = Logger.getLogger(TaskRunner.class);
    private final String name;
    private final ReentrantLock taskLifecycleLock = new ReentrantLock();
    private final State state;
    private final SourceTaskFactory sourceTaskFactory;
    private volatile boolean running;
    private volatile boolean reconfigurationNeeded;
    private SourceTask task;
    private Map<String, String> taskConfig;


    TaskRunner(String name, State state, SourceTaskFactory sourceTaskFactory) {
        this.name = name;
        this.state = state;
        this.sourceTaskFactory = sourceTaskFactory;
    }

    List<SourceRecord> poll() {
        restartTaskIfNeeded();
        if (running) {
            return doPoll();
        } else {
            return Collections.emptyList();
        }
    }

    void stop() {
        try {
            taskLifecycleLock.lock();
            if (running) {
                LOGGER.fine("Stopping task '" + name + "'");
                task.stop();
                LOGGER.fine("Task '" + name + "' stopped");
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
                LOGGER.warning("Stopping task '" + name + "' failed but proceeding with re-start", ex);
            }
        }
        start();
    }

    void updateTaskConfig(Map<String, String> taskConfig) {
        try {
            taskLifecycleLock.lock();
            if (!Objects.equals(this.taskConfig, taskConfig)) {
                LOGGER.info("Updating task '" + name + "' configuration");
                this.taskConfig = taskConfig;
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
                if (taskConfig != null) {
                    SourceTask taskLocal = sourceTaskFactory.create();
                    LOGGER.info("Initializing task '" + name + "'");
                    taskLocal.initialize(new JetSourceTaskContext(taskConfig, state));
                    LOGGER.info("Starting task '" + name + "'");
                    taskLocal.start(taskConfig);
                    this.task = taskLocal;
                    running = true;
                } else {
                    LOGGER.finest("No task config for task '" + name + "'");
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
                LOGGER.warning("Interrupted while committing");
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final class JetSourceTaskContext implements SourceTaskContext {
        private final Map<String, String> taskConfig;
        private final State state;

        private JetSourceTaskContext(Map<String, String> taskConfig,
                                     State state) {
            this.taskConfig = taskConfig;
            this.state = state;
        }

        @Override
        public Map<String, String> configs() {
            return taskConfig;
        }

        @Override
        public OffsetStorageReader offsetStorageReader() {
            return new SourceOffsetStorageReader(state);
        }
    }

    public void commitRecord(SourceRecord rec) {
        state.commitRecord(rec);
        try {
            if (running) {
                task.commitRecord(rec, null);
            }
        } catch (InterruptedException ie) {
            LOGGER.warning("Interrupted while committing record");
            Thread.currentThread().interrupt();
        }
    }

    public State createSnapshot() {
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
