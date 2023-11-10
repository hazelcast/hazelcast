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
import com.hazelcast.topic.Message;
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
public class SourceConnectorWrapper {
    // The logger is accessed by multiple threads
    private volatile ILogger logger = Logger.getLogger(SourceConnectorWrapper.class);
    private final SourceConnector sourceConnector;
    private final int tasksMax;
    private TaskRunner taskRunner;
    private final ReentrantLock reconfigurationLock = new ReentrantLock();
    private final State state = new State();
    private final String name;
    private boolean isMasterProcessor;
    private TaskConfigPublisher taskConfigPublisher;
    private int processorOrder;
    private final AtomicBoolean receivedTaskConfiguration = new AtomicBoolean();

    public SourceConnectorWrapper(Properties propertiesFromUser) {
        String connectorClazz = checkRequiredProperty(propertiesFromUser, "connector.class");
        this.name = checkRequiredProperty(propertiesFromUser, "name");
        // If "tasks.max" is not from test data use default value
        tasksMax = Integer.parseInt(propertiesFromUser.getProperty("tasks.max" , "1"));
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


    public void setMasterProcessor(boolean masterProcessor) {
        isMasterProcessor = masterProcessor;
    }

    public void setProcessorOrder(int processorOrder) {
        this.processorOrder = processorOrder;
    }

    public void createTopic(HazelcastInstance hazelcastInstance, long executionId) {
        taskConfigPublisher = new TaskConfigPublisher(hazelcastInstance);
        taskConfigPublisher.createTopic(executionId);

        // All processors must listen the topic
        taskConfigPublisher.addListener(this::processMessage);
    }

    public void destroyTopic() {
        taskConfigPublisher.removeListeners();
        if (isMasterProcessor) {
            // Only master processor can destroy the topic
            taskConfigPublisher.destroyTopic();
        }
    }

    protected void publishTopic(TaskConfigTopic taskConfigTopic) {
        if (taskConfigPublisher != null) {
            logger.info("Publishing TaskConfigTopic");
            taskConfigPublisher.publish(taskConfigTopic);
        }
    }

    private void processMessage(Message<TaskConfigTopic> message) {
        logger.info("Received TaskConfigTopic topic");
        TaskConfigTopic taskConfigTopic = message.getMessageObject();
        processMessage(taskConfigTopic);
    }

    // Protected so that it can be called from tests to simulate received topic
    protected void processMessage(TaskConfigTopic taskConfigTopic) {
        // Update state
        state.setTaskConfigs(taskConfigTopic.getTaskConfigs());

        // Get my taskConfig
        Map<String, String> taskConfig = state.getTaskConfig(processorOrder);

        // Pass my taskConfig to taskRunner
        logger.info("Updating taskRunner with processorOrder = " + processorOrder
                    + " with taskConfig=" + taskConfig);

        taskRunner.updateTaskConfig(taskConfig);

        receivedTaskConfiguration.set(true);
    }

    public List<SourceRecord> poll() {
        return taskRunner.poll();
    }

    public void commitRecord(SourceRecord sourceRecord) {
        taskRunner.commitRecord(sourceRecord);
    }

    public State copyState() {
        return taskRunner.copyState();
    }

    public void restoreState(State state) {
        taskRunner.restoreState(state);
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

    @SuppressWarnings({"ConstantConditions", "java:S1193"})
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
        String taskName = name + "-task-" + processorOrder;
        taskRunner = new TaskRunner(taskName, state, this::createSourceTask);
        taskRunner.setLogger(logger);
        requestTaskReconfiguration();
        return taskRunner;
    }

    private SourceTask createSourceTask() {
        Class<? extends SourceTask> taskClass = sourceConnector.taskClass().asSubclass(SourceTask.class);
        return newInstance(Thread.currentThread().getContextClassLoader(), taskClass.getName());
    }

    /**
     * This method is called from two different places
     * <p>
     * 1. When Connector starts for the first time, it calls this method to distribute taskConfigs
     * <p>
     * 2. When Kafka Connect plugin discovers that task reconfiguration is necessary
     */
    void requestTaskReconfiguration() {
        if (!isMasterProcessor) {
            logger.fine("requestTaskReconfiguration is skipped because Source Connector is not master");
            return;
        }
        try {
            reconfigurationLock.lock();
            logger.fine("Updating tasks configuration");
            List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(tasksMax);

            // Log taskConfigs
            for (int index = 0; index < taskConfigs.size(); index++) {
                Map<String, String> map = taskConfigs.get(index);
                logger.fine("sourceConnector index " + index + " taskConfig=" + map);
            }
            // Publish taskConfigs
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
               ", tasksMax=" + tasksMax +
               ", isMasterProcessor=" + isMasterProcessor +
               ", processorOrder=" + processorOrder +
               ", receivedTaskConfiguration=" + receivedTaskConfiguration +
               '}';
    }

    public boolean hasTaskRunner() {
        return taskRunner != null;
    }

    private class JetConnectorContext implements ConnectorContext {
        @Override
        public void requestTaskReconfiguration() {
            SourceConnectorWrapper.this.requestTaskReconfiguration();
        }

        @Override
        public void raiseError(Exception e) {
            throw rethrow(e);
        }
    }
}
