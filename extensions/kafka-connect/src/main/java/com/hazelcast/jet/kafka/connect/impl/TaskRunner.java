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
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;

class TaskRunner {
    private static final ILogger LOGGER = Logger.getLogger(TaskRunner.class);
    private final SourceConnector connector;
    private final ReentrantLock taskLifecycleLock = new ReentrantLock();
    private final Map<Map<String, ?>, Map<String, ?>> partitionsToOffset;
    private volatile boolean running;
    private volatile boolean reconfigurationRequested;
    private SourceTask task;

    TaskRunner(SourceConnector connector, Map<Map<String, ?>, Map<String, ?>> partitionsToOffset) {
        this.connector = connector;
        this.partitionsToOffset = partitionsToOffset;
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
            task = null;
            taskLifecycleLock.unlock();
        }
    }

    void requestReconfiguration() {
        LOGGER.info("Task reconfiguration requested");
        reconfigurationRequested = true;
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
        if (reconfigurationRequested) {
            reconfigurationRequested = false;
            stop();
        }
        start();
    }

    private void start() {
        LOGGER.info("Starting tasks if the previous not running");
        try {
            taskLifecycleLock.lock();
            if (!running) {
                LOGGER.info("Starting tasks");

                List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
                if (!taskConfigs.isEmpty()) {
                    Map<String, String> taskConfig = taskConfigs.get(0);
                    Class<? extends SourceTask> taskClass = connector.taskClass().asSubclass(SourceTask.class);
                    this.task = newInstance(Thread.currentThread().getContextClassLoader(), taskClass.getName());
                    LOGGER.info("Initializing task: " + task);
                    task.initialize(new JetSourceTaskContext(taskConfig));
                    LOGGER.info("Starting task: " + task + " with task config: " + taskConfig);
                    task.start(taskConfig);
                    running = true;
                } else {
                    LOGGER.info("No task configs!");
                }
            }
        } finally {
            taskLifecycleLock.unlock();
        }
    }

    private final class JetSourceTaskContext implements SourceTaskContext {
        private final Map<String, String> taskConfig;

        private JetSourceTaskContext(Map<String, String> taskConfig) {
            this.taskConfig = taskConfig;
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
}
