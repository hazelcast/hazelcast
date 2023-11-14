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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.client.impl.protocol.util.PropertiesUtil.toMap;
import static com.hazelcast.internal.util.Preconditions.checkRequiredProperty;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;

public class ConnectorWrapper {
    // job id to instance on given member
    private static final Map<Long, ConnectorWrapper> ACTIVE_CONNECTORS = new ConcurrentHashMap<>();
    private static final ILogger LOGGER = Logger.getLogger(ConnectorWrapper.class);
    private final SourceConnector connector;
    private final int tasksMax;
    private final State state = new State();
    private final String name;
    private final Runnable onReconfigurationFn;
    private final long jobId;
    private final List<TaskRunner> runners = new CopyOnWriteArrayList<>();
    private final ReentrantLock reconfigurationLock = new ReentrantLock();

    public ConnectorWrapper(long jobId, Properties properties) {
        this.jobId = jobId;
        this.onReconfigurationFn = this::requestReconfiguration;
        String connectorClazz = checkRequiredProperty(properties, "connector.class");
        this.name = checkRequiredProperty(properties, "name");
        this.tasksMax = Integer.parseInt(properties.getProperty("tasks.max", "1"));
        this.connector = newConnectorInstance(connectorClazz);

        LOGGER.fine("Initializing connector '" + name + "'");
        this.connector.initialize(new JetConnectorContext());

        LOGGER.fine("Starting connector '" + name + "'");
        this.connector.start(toMap(properties));

        // maybe execution id?
        ACTIVE_CONNECTORS.put(jobId, this);
    }

    private void requestReconfiguration() {
        reconfigurationLock.lock();
        try {
            for (TaskRunner runner : runners) {
                runner.restartTask(createConfig(runner.processorIndex));
            }
        } finally {
            reconfigurationLock.unlock();
        }
    }

    private static SourceConnector newConnectorInstance(String connectorClazz) {
        try {
            return newInstance(Thread.currentThread().getContextClassLoader(), connectorClazz);
        } catch (Exception e) {
            //noinspection ConstantValue ClassNotFoundException is possible
            if (e instanceof ClassNotFoundException) {
                throw new HazelcastException("Connector class '" + connectorClazz + "' not found. " +
                        "Did you add the connector jar to the job?", e);
            }
            throw rethrow(e);
        }
    }


    public void stop() {
        reconfigurationLock.lock();
        try {
            ACTIVE_CONNECTORS.remove(jobId);
            for (TaskRunner runner : runners) {
                runner.stop();
            }
            LOGGER.fine("Stopping connector '" + name + "'");
            connector.stop();
            LOGGER.fine("Connector '" + name + "' stopped");
        } finally {
            reconfigurationLock.unlock();
        }
    }

    TaskRunner createTaskRunner(int processorIndex) {
        String taskName = name + "-task-" + processorIndex;
        var taskConfig = createConfig(processorIndex);
        TaskRunner taskRunner = new TaskRunner(taskName, state, taskConfig, processorIndex, this::createSourceTask);
        runners.add(taskRunner);
        return taskRunner;
    }

    private Map<String, String> createConfig(int processorIndex) {
        // we request tasksMax == totalParallelism
        // however, taskConfigs may return AT MOST tasksMax configs
        // so if our index < taskConfigs, then we become basically NoOp
        List<Map<String, String>> taskConfigs = connector.taskConfigs(tasksMax);
        LOGGER.info("TAKING CONF " + processorIndex + " FROM " + tasksMax + " and size " + taskConfigs.size());
        if (taskConfigs.size() <= processorIndex) {
            return null;
        }
        var taskConfig = taskConfigs.get(processorIndex);
        return taskConfig;
    }

    private SourceTask createSourceTask() {
        Class<? extends SourceTask> taskClass = connector.taskClass().asSubclass(SourceTask.class);
        return newInstance(Thread.currentThread().getContextClassLoader(), taskClass.getName());
    }

    private class JetConnectorContext implements ConnectorContext {

        @Override
        public void requestTaskReconfiguration() {
            ConnectorWrapper.this.onReconfigurationFn.run();
        }

        @Override
        public void raiseError(Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public String toString() {
        return "ConnectorWrapper{" +
                "name='" + name + '\'' +
                '}';
    }
}
