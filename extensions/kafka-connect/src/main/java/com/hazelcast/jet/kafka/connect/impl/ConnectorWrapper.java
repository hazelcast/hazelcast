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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.kafka.connect.impl.topic.TaskConfigPublisher;
import com.hazelcast.jet.kafka.connect.impl.topic.TaskConfigTopic;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.topic.ReliableMessageListener;
import com.hazelcast.topic.impl.reliable.ReliableMessageListenerAdapter;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.client.impl.protocol.util.PropertiesUtil.toMap;
import static com.hazelcast.internal.util.Preconditions.checkRequiredProperty;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;

/**
 * This class wraps a Kafka Connector and TaskRunner
 */
public class ConnectorWrapper {
    private ILogger logger = Logger.getLogger(ConnectorWrapper.class);
    private final SourceConnector sourceConnector;
    private final int tasksMax;
    private TaskRunner taskRunner;
    private final ReentrantLock reconfigurationLock = new ReentrantLock();
    private int taskId;
    private final State state = new State();
    private final String name;
    private boolean isMasterProcessor;
    private TaskConfigPublisher taskConfigPublisher;
    private int processorOrder;
    private final AtomicBoolean receivedTaskConfiguration = new AtomicBoolean();

    public ConnectorWrapper(Properties propertiesFromUser) {
        String connectorClazz = checkRequiredProperty(propertiesFromUser, "connector.class");
        this.name = checkRequiredProperty(propertiesFromUser, "name");
        tasksMax = Integer.parseInt(propertiesFromUser.getProperty("tasks.max"));
        this.sourceConnector = newConnectorInstance(connectorClazz);
        logger.fine("Initializing connector '" + name + "'");
        this.sourceConnector.initialize(new JetConnectorContext());
        logger.fine("Starting connector '" + name + "'. Below are the propertiesFromUser");
        Map<String, String> map = toMap(propertiesFromUser);
        this.sourceConnector.start(map);
    }

    public boolean hasTaskConfiguration() {
        return receivedTaskConfiguration.get();
    }

    public void setLogger(ILogger logger) {
        this.logger = logger;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public void setMasterProcessor(boolean masterProcessor) {
        isMasterProcessor = masterProcessor;
    }

    public void setProcessorOrder(int processorOrder) {
        this.processorOrder = processorOrder;
    }

    public void createTopic(HazelcastInstance hazelcastInstance, long executionId) {
        taskConfigPublisher = new TaskConfigPublisher();
        taskConfigPublisher.createTopic(hazelcastInstance, executionId);

        // All processors must listen the topic
        addTopicListener(new ReliableMessageListenerAdapter<>(message -> {
            TaskConfigTopic taskConfigTopic = message.getMessageObject();
            state.setTaskConfigs(taskConfigTopic.getTaskConfigs());
            Map<String, String> taskConfig = state.getTaskConfig(processorOrder);

            logger.info("Updating taskRunner with processorOrder = " + processorOrder
                        + " taskConfigTopic=" + taskConfig);

            taskRunner.updateTaskConfig(taskConfig);

            receivedTaskConfiguration.set(true);
        }));
    }

    public void destroyTopic() {
        removeTopicListeners();
        if (isMasterProcessor) {
            // Only master processor can destroy the topic
            taskConfigPublisher.destroyTopic();
        }
    }

    private void addTopicListener(ReliableMessageListener<TaskConfigTopic> reliableMessageListener) {
        taskConfigPublisher.addListener(reliableMessageListener);
    }

    private void removeTopicListeners() {
        taskConfigPublisher.removeListeners();
    }

    private void publishTopic(TaskConfigTopic taskConfigTopic) {
        logger.fine("Publishing TaskConfigTopic");
        if (taskConfigPublisher != null) {
            taskConfigPublisher.publish(taskConfigTopic);
        }
    }

    public List<SourceRecord> poll() {
        return taskRunner.poll();
    }

    public void commitRecord(SourceRecord sourceRecord) {
        taskRunner.commitRecord(sourceRecord);
    }

    public State getSnapshotCopy() {
        return taskRunner.getSnapshotCopy();
    }

    public void restoreSnapshot(State state) {
        taskRunner.restoreSnapshot(state);
    }

    public void commit() {
        taskRunner.commit();
    }

    public String getName() {
        return taskRunner.getName();
    }

    public void taskRunnerStop() {
        taskRunner.stop();
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
        logger.fine("Stopping connector '" + name + "'");
        sourceConnector.stop();
        logger.fine("Connector '" + name + "' stopped");
    }

    public TaskRunner createTaskRunner() {
        String taskName = name + "-task-" + taskId;
        taskRunner = new TaskRunner(taskName, state, this::createSourceTask);
        taskRunner.setLogger(logger);
        requestTaskReconfiguration();
        return taskRunner;
    }

    private SourceTask createSourceTask() {
        Class<? extends SourceTask> taskClass = sourceConnector.taskClass().asSubclass(SourceTask.class);
        return newInstance(Thread.currentThread().getContextClassLoader(), taskClass.getName());
    }

    public void requestTaskReconfiguration() {
        if (!isMasterProcessor) {
            logger.fine("requestTaskReconfiguration is skipped");
            return;
        }
        try {
            reconfigurationLock.lock();
            logger.fine("Updating tasks configuration");
            List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(tasksMax);
            for (int index = 0; index < taskConfigs.size(); index++) {
                Map<String, String> map = taskConfigs.get(index);
                logger.fine("sourceConnector index " + index + " taskConfig=" + map);
            }
            TaskConfigTopic taskConfigTopic = new TaskConfigTopic();
            taskConfigTopic.setTaskConfigs(taskConfigs);
            publishTopic(taskConfigTopic);
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

    public boolean hasTaskRunner() {
        return taskRunner != null;
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
}
