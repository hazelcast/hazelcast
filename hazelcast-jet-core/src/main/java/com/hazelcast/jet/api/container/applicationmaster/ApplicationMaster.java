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

package com.hazelcast.jet.api.container.applicationmaster;


import com.hazelcast.jet.api.container.Container;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.ProcessingContainer;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterEvent;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.nio.Address;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Interface for JET application master
 * Application master - is abstract container to manage:
 * <pre>
 *     1) Other containers
 *     2) LifeCycle of the application
 * </pre>
 */
public interface ApplicationMaster extends Container<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse> {
    /**
     * Handle event when some container has been interrupted;
     *
     * @param error - corresponding error;
     */
    void handleContainerInterrupted(Throwable error);

    /**
     * Handle event when some container has been completed;
     */
    void handleContainerCompleted();

    /**
     * Register application execution;
     */
    void registerExecution();

    /**
     * Register application interruption;
     */
    void registerInterruption();

    /**
     * Handles some event during execution;
     *
     * @param error - corresponding error;
     */
    void notifyExecutionError(Object error);

    /**
     * @return - mailBox which is used to signal that application's;
     * execution has been completed;
     */
    BlockingQueue<Object> getExecutionMailBox();

    /**
     * @return - mailBox which is used to signal that application's;
     * interruption has been completed;
     */
    BlockingQueue<Object> getInterruptionMailBox();

    /**
     * @param vertex the vertex to get the container for
     * @return - processing container for the corresponding vertex;
     */
    ProcessingContainer getContainerByVertex(Vertex vertex);

    /**
     * Register container for the specified vertex;
     *
     * @param vertex    - corresponding vertex;
     * @param container - corresponding container;
     */
    void registerContainer(Vertex vertex, ProcessingContainer container);

    /**
     * @return - map with containers;
     */
    Map<Integer, ProcessingContainer> getContainersCache();

    /**
     * @return - list of containers;
     */
    List<ProcessingContainer> containers();

    /**
     * Register shuffling receiver for the corresponding task and address;
     *
     * @param taskID           - corresponding taskID
     * @param containerContext - corresponding container context;
     * @param address          - corresponding address;
     * @param receiver         - registered receiver;
     */
    void registerShufflingReceiver(int taskID, ContainerContext containerContext, Address address, ShufflingReceiver receiver);

    /**
     * Register shuffling receiver for the corresponding task and address;
     *
     * @param taskID           - corresponding taskID
     * @param containerContext - corresponding container context;
     * @param address          - corresponding address;
     * @param sender           - registered sender;
     */
    void registerShufflingSender(int taskID, ContainerContext containerContext, Address address, ShufflingSender sender);

    /**
     * @return - dag of the application;
     */
    DAG getDag();

    /**
     * Set up dag for the corresponding application;
     *
     * @param dag - corresponding dag;
     */
    void setDag(DAG dag);

    /**
     * @return - name of the application;
     */
    String getApplicationName();

    /**
     * Notify all application's containers with some signal;
     *
     * @param reason - signal object;
     */
    void notifyContainers(Object reason);

    /**
     * Deploys network engine;
     */
    void deployNetworkEngine();

    /**
     * Invoked by network task on task's finish
     */
    void notifyNetworkTaskFinished();
}
