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

package com.hazelcast.jet.api.container;

import com.hazelcast.jet.api.container.task.TaskEvent;
import com.hazelcast.jet.api.processor.ContainerProcessorFactory;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerEvent;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerResponse;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerState;
import com.hazelcast.jet.spi.dag.Vertex;

import java.util.List;
import java.util.Map;

/**
 * Represents abstract processing container;
 */
public interface ProcessingContainer
        extends Container<ProcessingContainerEvent, ProcessingContainerState, ProcessingContainerResponse> {
    /**
     * @return - user-level container processing factory;
     */
    ContainerProcessorFactory getContainerProcessorFactory();

    /**
     * @return - list of the input channels;
     */
    List<DataChannel> getInputChannels();

    /**
     * @return - list of the output channels;
     */
    List<DataChannel> getOutputChannels();

    /**
     * Add input channel for container;
     *
     * @param channel - corresponding channel;
     */
    void addInputChannel(DataChannel channel);

    /**
     * Add output channel for container;
     *
     * @param channel - corresponding channel;
     */
    void addOutputChannel(DataChannel channel);

    /**
     * Handles task's event;
     *
     * @param containerTask - corresponding container task;
     * @param event         - task's event;
     */
    void handleTaskEvent(ContainerTask containerTask, TaskEvent event);

    /**
     * Handles task's error-event;
     *
     * @param containerTask - corresponding container task;
     * @param event         - task's event;
     * @param error         - corresponding error;
     */
    void handleTaskEvent(ContainerTask containerTask, TaskEvent event, Throwable error);

    /**
     * TaskID -> ContainerTask;
     *
     * @return - cache of container's tasks;
     */
    Map<Integer, ContainerTask> getTasksCache();

    /**
     * @return - tasks of the container;
     */
    ContainerTask[] getContainerTasks();

    /**
     * @return - vertex for the corresponding container;
     */
    Vertex getVertex();

    /**
     * Starts containers execution;
     *
     * @throws Exception;
     */
    void start() throws Exception;

    /**
     * Destroys container;
     *
     * @throws Exception;
     */
    void destroy() throws Exception;

    /**
     * Interrupt container's execution
     *
     * @param error - corresponding error;
     */
    void interrupt(Throwable error);
}
