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

package com.hazelcast.jet.internal.impl.container;


import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.internal.api.application.ApplicationContext;
import com.hazelcast.jet.internal.api.application.ApplicationException;
import com.hazelcast.jet.internal.api.container.ContainerContext;
import com.hazelcast.jet.internal.api.container.ContainerPayLoadProcessor;
import com.hazelcast.jet.internal.api.container.ContainerTask;
import com.hazelcast.jet.internal.api.container.DiscoveryService;
import com.hazelcast.jet.internal.api.container.ProcessingContainer;
import com.hazelcast.jet.internal.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.internal.api.statemachine.container.applicationmaster.ApplicationMasterEvent;
import com.hazelcast.jet.internal.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.internal.api.statemachine.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.jet.internal.api.statemachine.container.applicationmaster.ApplicationMasterStateMachineFactory;
import com.hazelcast.jet.internal.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.jet.internal.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.internal.impl.hazelcast.JetPacket;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.processors.ApplicationMasterPayLoadFactory;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.ExecutionCompletedRequest;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.ExecutionErrorRequest;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.ExecutionInterruptedRequest;
import com.hazelcast.jet.internal.impl.util.JetUtil;
import com.hazelcast.jet.api.CombinedJetException;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.nio.Address;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ApplicationMasterImpl extends
        AbstractServiceContainer<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse>
        implements ApplicationMaster {

    private static final InterruptedException APPLICATION_INTERRUPTED_EXCEPTION =
            new InterruptedException("Application has been interrupted");

    private static final ApplicationMasterStateMachineFactory STATE_MACHINE_FACTORY =
            new DefaultApplicationMasterStateMachineFactory();

    private final List<ProcessingContainer> containers = new CopyOnWriteArrayList<ProcessingContainer>();
    private final Map<Integer, ProcessingContainer> containersCache = new ConcurrentHashMap<Integer, ProcessingContainer>();

    private final Map<Vertex, ProcessingContainer> vertex2ContainerCache = new ConcurrentHashMap<Vertex, ProcessingContainer>();

    private final AtomicInteger containerCounter = new AtomicInteger(0);

    private final AtomicInteger networkTaskCounter = new AtomicInteger(0);
    private final AtomicReference<BlockingQueue<Object>> executionMailBox =
            new AtomicReference<BlockingQueue<Object>>(null);
    private final AtomicReference<BlockingQueue<Object>> interruptionFutureHolder =
            new AtomicReference<BlockingQueue<Object>>(null);
    private final DiscoveryService discoveryService;
    private volatile boolean interrupted;
    private volatile Throwable interruptionError;
    private volatile DAG dag;
    private final byte[] applicationNameBytes;

    public ApplicationMasterImpl(
            ApplicationContext applicationContext,
            DiscoveryService discoveryService
    ) {
        super(STATE_MACHINE_FACTORY, applicationContext.getNodeEngine(), applicationContext);
        this.discoveryService = discoveryService;
        this.applicationNameBytes = getNodeEngine().getSerializationService()
                .toData(applicationContext.getName()).toByteArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processRequest(ApplicationMasterEvent event, Object payLoad) throws Exception {
        ContainerPayLoadProcessor processor = ApplicationMasterPayLoadFactory.getProcessor(event, this);

        if (processor != null) {
            processor.process(payLoad);
        }
    }

    @Override
    public void handleContainerCompleted() {
        if (this.containerCounter.incrementAndGet() >= this.containers.size()) {
            if (getNetworkTaskCount() > 0) {
                this.getApplicationContext().getExecutorContext().getNetworkTaskContext().finalizeTasks();
            } else {
                notifyCompletionFinished();
            }
        }
    }

    @Override
    public void handleContainerInterrupted(Throwable error) {
        this.interrupted = true;
        this.interruptionError = error;
        handleContainerCompleted();
    }

    private void notifyCompletionFinished() {
        try {
            if (this.interrupted) {
                notifyInterrupted(this.interruptionError);
            } else {
                try {
                    handleContainerRequest(new ExecutionCompletedRequest()).get(
                            getApplicationContext().getJetApplicationConfig().getJetSecondsToAwait(),
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
        return
                this.getApplicationContext().getExecutorContext().
                        getNetworkTaskContext().getTasks().length;
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private void notifyInterrupted(Throwable error) throws Exception {
        Throwable interruptionError =
                error == null ? APPLICATION_INTERRUPTED_EXCEPTION : error;

        try {
            try {
                handleContainerRequest(new ExecutionInterruptedRequest()).get(
                        getApplicationContext().getJetApplicationConfig().getJetSecondsToAwait(),
                        TimeUnit.SECONDS
                );
            } finally {
                if (this.interruptionFutureHolder.get() != null) {
                    this.interruptionFutureHolder.get().offer(true);
                }
            }
        } finally {
            addToExecutionMailBox(interruptionError);
        }
    }

    @Override
    public void notifyNetworkTaskFinished() {
        if (this.networkTaskCounter.incrementAndGet() >= getNetworkTaskCount()) {
            notifyCompletionFinished();
        }
    }

    @Override
    public void registerExecution() {
        this.interrupted = false;
        this.interruptionError = null;
        this.containerCounter.set(0);
        this.networkTaskCounter.set(0);
        this.executionMailBox.set(new ArrayBlockingQueue<Object>(1));
    }

    @Override
    public void registerInterruption() {
        this.interruptionFutureHolder.set(new ArrayBlockingQueue<Object>(1));
    }

    @Override
    public BlockingQueue<Object> getExecutionMailBox() {
        return this.executionMailBox.get();
    }

    @Override
    public BlockingQueue<Object> getInterruptionMailBox() {
        return this.interruptionFutureHolder.get();
    }

    @Override
    public List<ProcessingContainer> containers() {
        return Collections.unmodifiableList(this.containers);
    }

    private void addToExecutionMailBox(Object object) {
        addToMailBox(object);
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private void addToMailBox(Object object) {
        BlockingQueue<Object> executionMailBox = this.executionMailBox.get();

        if (executionMailBox != null) {
            executionMailBox.offer(object);
        }
    }

    @Override
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
                this.applicationNameBytes,
                toBytes(reason)
        );

        jetPacket.setHeader(JetPacket.HEADER_JET_EXECUTION_ERROR);
        Address jetAddress = getApplicationContext().getHzToJetAddressMapping().get(member.getAddress());
        this.discoveryService.getSocketWriters().get(jetAddress).sendServicePacket(jetPacket);
    }

    @Override
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
                    ?
                    new ApplicationException(initiator)
                    :
                    toException(initiator, (JetPacket) reason);
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

    @Override
    public void deployNetworkEngine() {
        this.discoveryService.executeDiscovery();
    }

    private byte[] toBytes(Object reason) {
        if (reason == null) {
            return null;
        }

        return ((InternalSerializationService) getNodeEngine().getSerializationService()).toBytes(reason);
    }

    @Override
    public void registerShufflingReceiver(int taskID,
                                          ContainerContext containerContext,
                                          Address address,
                                          ShufflingReceiver receiver) {
        ProcessingContainer processingContainer = this.containersCache.get(containerContext.getID());
        ContainerTask containerTask = processingContainer.getTasksCache().get(taskID);
        containerTask.registerShufflingReceiver(address, receiver);
        this.discoveryService.getSocketReaders().get(address).registerConsumer(receiver.getRingBufferActor());
    }

    @Override
    public void registerShufflingSender(int taskID,
                                        ContainerContext containerContext,
                                        Address address,
                                        ShufflingSender sender) {
        ProcessingContainer processingContainer = this.containersCache.get(containerContext.getID());
        ContainerTask containerTask = processingContainer.getTasksCache().get(taskID);
        containerTask.registerShufflingSender(address, sender);
        this.discoveryService.getSocketWriters().get(address).registerProducer(sender.getRingBufferActor());
    }

    @Override
    public Map<Integer, ProcessingContainer> getContainersCache() {
        return this.containersCache;
    }

    @Override
    public DAG getDag() {
        return this.dag;
    }

    @Override
    public void setDag(DAG dag) {
        this.dag = dag;
    }

    @Override
    public ProcessingContainer getContainerByVertex(Vertex vertex) {
        return this.vertex2ContainerCache.get(vertex);
    }

    @Override
    public void registerContainer(Vertex vertex, ProcessingContainer container) {
        this.containers.add(container);
        this.vertex2ContainerCache.put(vertex, container);
        this.containersCache.put(container.getID(), container);
    }

    @Override
    protected void wakeUpExecutor() {
        getApplicationContext().getExecutorContext().getApplicationMasterStateMachineExecutor().wakeUp();
    }
}
