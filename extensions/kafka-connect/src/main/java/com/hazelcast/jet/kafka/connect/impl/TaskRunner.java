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

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.Collections.emptyList;

interface TaskRunner {
    List<SourceRecord> poll();
    void start();
    void commit();

    void commitRecord(SourceRecord rec);
    void stop();

    State createSnapshot();

    void restoreSnapshot(State state);

    String name();
}

class DefaultTaskRunner implements TaskRunner {
    private static final ILogger LOGGER = Logger.getLogger(TaskRunner.class);
    private final String name;
    private final ReentrantLock taskLifecycleLock = new ReentrantLock();
    private final State state;
    private final SourceTaskFactory sourceTaskFactory;
    private final Map<String, String> taskConfig;
    private volatile boolean running;
    private SourceTask task;

    DefaultTaskRunner(String name, State state, Map<String, String> taskConfig, SourceTaskFactory sourceTaskFactory) {
        this.name = name;
        this.state = state;
        this.sourceTaskFactory = sourceTaskFactory;
        this.taskConfig = taskConfig;
    }

    @Override
    public List<SourceRecord> poll() {
        if (running) {
            return doPoll();
        } else {
            return emptyList();
        }
    }

    @Override
    public void stop() {
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
            return sourceRecords == null ? emptyList() : sourceRecords;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw rethrow(e);
        }
    }

    @Override
    public void start() {
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

    @Override
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

    @Override
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

    @Override
    public State createSnapshot() {
        State snapshot = new State();
        snapshot.load(state);
        return snapshot;
    }

    @Override
    public void restoreSnapshot(State state) {
        this.state.load(state);
    }

    @Override
    public String name() {
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

class NoOpRunner implements TaskRunner {
    @Override
    public List<SourceRecord> poll() {
        return emptyList();
    }

    @Override
    public void stop() {
    }

    @Override
    public void start() {
    }

    @Override
    public void commit() {
    }

    @Override
    public void commitRecord(SourceRecord rec) {
    }

    @Override
    public State createSnapshot() {
        return null;
    }

    @Override
    public void restoreSnapshot(State state) {
    }

    @Override
    public String name() {
        return null;
    }
}
