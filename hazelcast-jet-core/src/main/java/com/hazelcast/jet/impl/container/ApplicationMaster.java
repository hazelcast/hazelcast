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

package com.hazelcast.jet.impl.container;


import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.CombinedJetException;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.jet.impl.application.ApplicationException;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMasterEvent;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.jet.impl.data.io.JetPacket;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.jet.impl.statemachine.StateMachine;
import com.hazelcast.jet.impl.statemachine.StateMachineFactory;
import com.hazelcast.jet.impl.statemachine.applicationmaster.ApplicationMasterStateMachine;
import com.hazelcast.jet.impl.statemachine.applicationmaster.processors.DestroyApplicationProcessor;
import com.hazelcast.jet.impl.statemachine.applicationmaster.processors.ExecuteApplicationProcessor;
import com.hazelcast.jet.impl.statemachine.applicationmaster.processors.ExecutionCompletedProcessor;
import com.hazelcast.jet.impl.statemachine.applicationmaster.processors.ExecutionErrorProcessor;
import com.hazelcast.jet.impl.statemachine.applicationmaster.processors.ExecutionPlanBuilderProcessor;
import com.hazelcast.jet.impl.statemachine.applicationmaster.processors.InterruptApplicationProcessor;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionCompletedRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionErrorRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionInterruptedRequest;
import com.hazelcast.jet.impl.util.BasicCompletableFuture;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class ApplicationMaster
        extends AbstractContainer<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse> {

    private static final InterruptedException APPLICATION_INTERRUPTED_EXCEPTION =
            new InterruptedException("Application has been interrupted");

    private static final StateMachineFactory<ApplicationMasterEvent,
            StateMachine<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse>>
            STATE_MACHINE_FACTORY = ApplicationMasterStateMachine::new;

    private final List<ProcessingContainer> containers = new CopyOnWriteArrayList<>();
    private final Map<Integer, ProcessingContainer> containersCache = new ConcurrentHashMap<>();

    private final Map<Vertex, ProcessingContainer> vertex2ContainerCache = new ConcurrentHashMap<>();

    private final AtomicInteger containerCounter = new AtomicInteger(0);

    private final AtomicInteger networkTaskCounter = new AtomicInteger(0);
    private final AtomicReference<BasicCompletableFuture<Object>> executionMailBox =
            new AtomicReference<>(null);
    private final AtomicReference<BasicCompletableFuture<Object>> interruptionFutureHolder =
            new AtomicReference<>(null);

    private final ILogger logger;
    private final DiscoveryService discoveryService;
    private volatile boolean interrupted;
    private volatile Throwable interruptionError;
    private volatile DAG dag;
    private final byte[] applicationNameBytes;

    public ApplicationMaster(
            ApplicationContext applicationContext,
            DiscoveryService discoveryService
    ) {
        super(STATE_MACHINE_FACTORY, applicationContext.getNodeEngine(), applicationContext, null);
        this.discoveryService = discoveryService;
        applicationNameBytes = getNodeEngine().getSerializationService()
                .toData(applicationContext.getName()).toByteArray();
        logger = getNodeEngine().getLogger(ApplicationMaster.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processRequest(ApplicationMasterEvent event, Object payload) throws Exception {
        ContainerPayloadProcessor processor = getProcessor(event, this);

        if (processor != null) {
            processor.process(payload);
        }
    }

    /**
     * Handle event when some container has been completed;
     */
    public void handleContainerCompleted() {
        if (containerCounter.incrementAndGet() >= containers.size()) {
            if (getNetworkTaskCount() > 0) {
                List<Task> networkTasks = getApplicationContext().getExecutorContext().getNetworkTasks();
                networkTasks.forEach(Task::finalizeTask);
            } else {
                notifyCompletionFinished();
            }
        }
    }

    /**
     * Handle event when some container has been interrupted;
     *
     * @param error corresponding error;
     */
    public void handleContainerInterrupted(Throwable error) {
        logger.info("handle container interrupted " + error);
        interrupted = true;
        interruptionError = error;
        handleContainerCompleted();
    }

    private void notifyCompletionFinished() {
        try {
            if (interrupted) {
                notifyInterrupted(interruptionError);
            } else {
                try {
                    handleContainerRequest(new ExecutionCompletedRequest()).get(
                            getApplicationContext().getApplicationConfig().getSecondsToAwait(),
                            TimeUnit.SECONDS
                    );
                } finally {
                    addToExecutionMailBox(true);
                }
            }
        } catch (Throwable e) {
            throw JetUtil.reThrow(e);
        }
    }

    private int getNetworkTaskCount() {
        return getApplicationContext().getExecutorContext().getNetworkTasks().size();
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private void notifyInterrupted(Throwable error) throws Exception {
        logger.info("notify interrupted " + error);

        Throwable interruptionError =
                error == null ? APPLICATION_INTERRUPTED_EXCEPTION : error;

        try {
            try {
                handleContainerRequest(new ExecutionInterruptedRequest()).get(
                        getApplicationContext().getApplicationConfig().getSecondsToAwait(),
                        TimeUnit.SECONDS
                );
            } finally {
                if (interruptionFutureHolder.get() != null) {
                    interruptionFutureHolder.get().setResult(true);
                }
            }
        } finally {
            addToExecutionMailBox(interruptionError);
        }
    }

    /**
     * Invoked by network task on task's finish
     */
    public void notifyNetworkTaskFinished() {
        if (networkTaskCounter.incrementAndGet() >= getNetworkTaskCount()) {
            notifyCompletionFinished();
        }
    }

    /**
     * Register application execution;
     */
    public void registerExecution() {
        interrupted = false;
        interruptionError = null;
        containerCounter.set(0);
        networkTaskCounter.set(0);
        executionMailBox.set(new BasicCompletableFuture<>(getNodeEngine(), logger));
    }

    /**
     * Register application interruption;
     */
    public void registerInterruption() {
        interruptionFutureHolder.set(new BasicCompletableFuture<>(getNodeEngine(), logger));
    }

    /**
     * @return mailBox which is used to signal that application's;
     * execution has been completed;
     */
    public ICompletableFuture<Object> getExecutionMailBox() {
        return executionMailBox.get();
    }

    /**
     * @return mailBox which is used to signal that application's;
     * interruption has been completed;
     */
    public ICompletableFuture<Object> getInterruptionMailBox() {
        return interruptionFutureHolder.get();
    }

    /**
     * @return list of containers;
     */
    public List<ProcessingContainer> containers() {
        return Collections.unmodifiableList(containers);
    }

    private void addToExecutionMailBox(Object object) {
        addToMailBox(object);
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private void addToMailBox(Object object) {
        BasicCompletableFuture<Object> mailbox = executionMailBox.get();

        if (mailbox != null) {
            mailbox.setResult(object);
        }
    }

    /**
     * Handles some event during execution;
     *
     * @param reason corresponding error;
     */
    public void notifyExecutionError(Object reason) {
        for (MemberImpl member : getNodeEngine().getClusterService().getMemberImpls()) {
            if (!member.localMember()) {
                notifyClusterMembers(reason, member);
            } else {
                notifyContainers(reason);
            }
        }
    }

    private void notifyClusterMembers(Object reason, MemberImpl member) {
        JetPacket jetPacket = new JetPacket(
                applicationNameBytes,
                toBytes(reason)
        );

        jetPacket.setHeader(JetPacket.HEADER_JET_EXECUTION_ERROR);
        Address jetAddress = getApplicationContext().getHzToJetAddressMapping().get(member.getAddress());
        discoveryService.getSocketWriters().get(jetAddress).sendServicePacket(jetPacket);
    }

    /**
     * Notify all application's containers with some signal;
     *
     * @param reason signal object;
     */
    public void notifyContainers(Object reason) {
        Throwable error = getError(reason);
        handleContainerRequest(
                new ExecutionErrorRequest(error)
        );
    }

    private Throwable getError(Object reason) {
        Throwable error;

        if (reason instanceof JetPacket) {
            JetPacket packet = (JetPacket) reason;
            Address initiator = packet.getRemoteMember();

            error = packet.toByteArray() == null
                    ? new ApplicationException(initiator) : toException(initiator, (JetPacket) reason);
        } else if (reason instanceof Address) {
            error = new ApplicationException((Address) reason);
        } else if (reason instanceof Throwable) {
            error = (Throwable) reason;
        } else {
            error = new ApplicationException(reason, getNodeEngine().getLocalMember().getAddress());
        }

        return error;
    }

    private Throwable toException(Address initiator, JetPacket packet) {
        Object object = getNodeEngine().getSerializationService().toObject(new HeapData(packet.toByteArray()));

        if (object instanceof Throwable) {
            return new CombinedJetException(Arrays.asList((Throwable) object, new ApplicationException(initiator)));
        } else if (object instanceof JetPacket) {
            return new ApplicationException(initiator, (JetPacket) object);
        } else {
            return new ApplicationException(object, initiator);
        }
    }

    /**
     * Deploys network engine;
     */
    public void deployNetworkEngine() {
        discoveryService.executeDiscovery();
    }

    private byte[] toBytes(Object reason) {
        if (reason == null) {
            return null;
        }

        return ((InternalSerializationService) getNodeEngine().getSerializationService()).toBytes(reason);
    }

    /**
     * Register shuffling receiver for the corresponding task and address;
     *
     * @param taskID           corresponding taskID
     * @param containerContext corresponding container context;
     * @param address          corresponding address;
     * @param receiver         registered receiver;
     */
    public void registerShufflingReceiver(int taskID,
                                          ContainerContext containerContext,
                                          Address address,
                                          ShufflingReceiver receiver) {
        ProcessingContainer processingContainer = containersCache.get(containerContext.getID());
        ContainerTask containerTask = processingContainer.getTasksCache().get(taskID);
        containerTask.registerShufflingReceiver(address, receiver);
        discoveryService.getSocketReaders().get(address).registerConsumer(receiver.getRingBufferActor());
    }

    /**
     * Register shuffling receiver for the corresponding task and address;
     *
     * @param taskID           corresponding taskID
     * @param containerContext corresponding container context;
     * @param address          corresponding address;
     * @param sender           registered sender;
     */
    public void registerShufflingSender(int taskID,
                                        ContainerContext containerContext,
                                        Address address,
                                        ShufflingSender sender) {
        ProcessingContainer processingContainer = containersCache.get(containerContext.getID());
        ContainerTask containerTask = processingContainer.getTasksCache().get(taskID);
        containerTask.registerShufflingSender(address, sender);
        discoveryService.getSocketWriters().get(address).registerProducer(sender.getRingBufferActor());
    }

    /**
     * @return map with containers;
     */
    public Map<Integer, ProcessingContainer> getContainersCache() {
        return containersCache;
    }

    /**
     * @return dag of the application;
     */
    public DAG getDag() {
        return dag;
    }

    /**
     * Set up dag for the corresponding application;
     *
     * @param dag corresponding dag;
     */
    public void setDag(DAG dag) {
        this.dag = dag;
    }

    /**
     * @param vertex the vertex to get the container for
     * @return processing container for the corresponding vertex;
     */
    public ProcessingContainer getContainerByVertex(Vertex vertex) {
        return vertex2ContainerCache.get(vertex);
    }

    /**
     * Register container for the specified vertex;
     *
     * @param vertex    corresponding vertex;
     * @param container corresponding container;
     */
    public void registerContainer(Vertex vertex, ProcessingContainer container) {
        containers.add(container);
        vertex2ContainerCache.put(vertex, container);
        containersCache.put(container.getID(), container);
    }

    @Override
    protected void wakeUpExecutor() {
        getApplicationContext().getExecutorContext().getApplicationMasterStateMachineExecutor().wakeUp();
    }

    private static ContainerPayloadProcessor getProcessor(ApplicationMasterEvent event,
                                                          ApplicationMaster applicationMaster) {
        switch (event) {
            case SUBMIT_DAG:
                return new ExecutionPlanBuilderProcessor(applicationMaster);
            case EXECUTE:
                return new ExecuteApplicationProcessor(applicationMaster);
            case INTERRUPT_EXECUTION:
                return new InterruptApplicationProcessor(applicationMaster);
            case EXECUTION_ERROR:
                return new ExecutionErrorProcessor(applicationMaster);
            case EXECUTION_COMPLETED:
                return new ExecutionCompletedProcessor();
            case FINALIZE:
                return new DestroyApplicationProcessor(applicationMaster);
            default:
                return null;
        }
    }
}
