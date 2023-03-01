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

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

class TaskRunner {
    private static final ILogger LOGGER = Logger.getLogger(TaskRunner.class);
    private final ReentrantLock taskLifecycleLock = new ReentrantLock();
    private final Map<Map<String, ?>, Map<String, ?>> partitionsToOffset;
    private final SourceTaskFactory sourceTaskFactory;
    private volatile boolean running;
    private volatile boolean reconfigurationNeeded;
    private SourceTask task;
    private Map<String, String> newTaskConfig;

    TaskRunner(Map<Map<String, ?>, Map<String, ?>> partitionsToOffset, SourceTaskFactory sourceTaskFactory) {
        this.partitionsToOffset = partitionsToOffset;
        this.sourceTaskFactory = sourceTaskFactory;
    }

    List<SourceRecord> poll() {
        restartTaskIfNeeded();
        return doPoll();
    }

    void stop() {
        LOGGER.info("Stopping task if running");
        try {
            taskLifecycleLock.lock();
            if (running) {
                LOGGER.fine("Stopping task");
                task.stop();
                LOGGER.fine("Task stopped");
            }
        } finally {
            running = false;
            taskLifecycleLock.unlock();
        }
    }

    private List<SourceRecord> doPoll() {
        try {
            return task.poll();
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
                LOGGER.warning("Stopping task failed but proceeding with re-start", ex);
            }
        }
        start();
    }

    void updateTaskConfig(Map<String, String> taskConfig) {
        try {
            taskLifecycleLock.lock();
            this.newTaskConfig = taskConfig;
        } finally {
            taskLifecycleLock.unlock();
        }
        reconfigurationNeeded = true;
    }

    private void start() {
        LOGGER.info("Starting task if the previous not running");
        try {
            taskLifecycleLock.lock();
            if (!running) {
                if (newTaskConfig != null) {
                    SourceTask taskLocal = sourceTaskFactory.create();
                    LOGGER.info("Initializing task: " + task);
                    taskLocal.initialize(new JetSourceTaskContext(newTaskConfig, partitionsToOffset));
                    LOGGER.info("Starting task: " + taskLocal + " with task config: " + newTaskConfig);
                    taskLocal.start(newTaskConfig);
                    this.task = taskLocal;
                    running = true;
                } else {
                    LOGGER.info("No task config");
                }
            }
        } finally {
            taskLifecycleLock.unlock();
        }
    }

    private static final class JetSourceTaskContext implements SourceTaskContext {
        private final Map<String, String> taskConfig;
        private final Map<Map<String, ?>, Map<String, ?>> partitionsToOffset;

        private JetSourceTaskContext(Map<String, String> taskConfig,
                                     Map<Map<String, ?>, Map<String, ?>> partitionsToOffset) {
            this.taskConfig = taskConfig;
            this.partitionsToOffset = partitionsToOffset;
        }

        @Override
        public Map<String, String> configs() {
            return taskConfig;
        }

        @Override
        public OffsetStorageReader offsetStorageReader() {
            return new SourceOffsetStorageReader(partitionsToOffset);
        }
    }

    public void commitRecord(SourceRecord record) {
        partitionsToOffset.put(record.sourcePartition(), record.sourceOffset());
    }


    @FunctionalInterface
    interface SourceTaskFactory {
        SourceTask create();
    }
}
