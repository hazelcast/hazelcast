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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.client.impl.protocol.util.PropertiesUtil.toMap;
import static com.hazelcast.internal.util.Preconditions.checkRequiredProperty;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;

public class ConnectorWrapper {
    private static final ILogger LOGGER = Logger.getLogger(ConnectorWrapper.class);
    private final SourceConnector connector;
    private final int tasksMax;
    private final List<TaskRunner> taskRunners = new CopyOnWriteArrayList<>();
    private final AtomicInteger taskIdGenerator = new AtomicInteger();
    private final ReentrantLock reconfigurationLock = new ReentrantLock();
    private final State state = new State();
    private final String name;

    public ConnectorWrapper(Properties properties) {
        String connectorClazz = checkRequiredProperty(properties, "connector.class");
        this.name = checkRequiredProperty(properties, "name");
        tasksMax = Integer.parseInt(properties.getProperty("tasks.max", "1"));
        this.connector = newConnectorInstance(connectorClazz);
        LOGGER.fine("Initializing connector '" + name + "'");
        this.connector.initialize(new JetConnectorContext());
        LOGGER.fine("Starting connector '" + name + "'");
        this.connector.start(toMap(properties));
    }

    private static SourceConnector newConnectorInstance(String connectorClazz) {
        try {
            return newInstance(Thread.currentThread().getContextClassLoader(), connectorClazz);
        } catch (Exception e) {
            if (e instanceof ClassNotFoundException) {
                throw new HazelcastException("Connector class '" + connectorClazz + "' not found. " +
                        "Did you add the connector jar to the job?", e);
            }
            throw rethrow(e);
        }
    }

    public void stop() {
        LOGGER.fine("Stopping connector '" + name + "'");
        connector.stop();
        LOGGER.fine("Connector '" + name + "' stopped");

    }

    public TaskRunner createTaskRunner() {
        String taskName = name + "-task-" + taskIdGenerator.getAndIncrement();
        TaskRunner taskRunner = new TaskRunner(taskName, state, this::createSourceTask);
        taskRunners.add(taskRunner);
        requestTaskReconfiguration();
        return taskRunner;
    }

    private SourceTask createSourceTask() {
        Class<? extends SourceTask> taskClass = connector.taskClass().asSubclass(SourceTask.class);
        return newInstance(Thread.currentThread().getContextClassLoader(), taskClass.getName());
    }


    private class JetConnectorContext implements ConnectorContext {

        @Override
        public void requestTaskReconfiguration() {
            ConnectorWrapper.this.requestTaskReconfiguration();
        }

        @Override
        public void raiseError(Exception e) {
            throw rethrow(e);
        }
    }

    private void requestTaskReconfiguration() {
        if (taskRunners.isEmpty()) {
            return;
        }
        try {
            reconfigurationLock.lock();
            LOGGER.fine("Updating tasks configuration");
            int taskRunnersSize = taskRunners.size();
            List<Map<String, String>> taskConfigs = connector.taskConfigs(Math.min(tasksMax, taskRunnersSize));

            for (int i = 0; i < taskRunnersSize; i++) {
                Map<String, String> taskConfig = i < taskConfigs.size() ? taskConfigs.get(i) : null;
                taskRunners.get(i).updateTaskConfig(taskConfig);
            }
        } finally {
            reconfigurationLock.unlock();
        }
    }

    @Override
    public String toString() {
        return "ConnectorWrapper{" +
                "name='" + name + '\'' +
                '}';
    }
}
