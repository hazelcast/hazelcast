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

package com.hazelcast.jet.impl.runtime;


import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.CombinedJetException;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.ringbuffer.ShufflingReceiver;
import com.hazelcast.jet.impl.ringbuffer.ShufflingSender;
import com.hazelcast.jet.impl.data.io.JetPacket;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.job.JobException;
import com.hazelcast.jet.impl.runtime.jobmanager.JobManagerEvent;
import com.hazelcast.jet.impl.runtime.jobmanager.JobManagerResponse;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.impl.statemachine.StateMachineRequest;
import com.hazelcast.jet.impl.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.impl.statemachine.jobmanager.JobManagerStateMachine;
import com.hazelcast.jet.impl.statemachine.jobmanager.processors.DestroyJobProcessor;
import com.hazelcast.jet.impl.statemachine.jobmanager.processors.ExecuteJobProcessor;
import com.hazelcast.jet.impl.statemachine.jobmanager.processors.ExecutionCompletedProcessor;
import com.hazelcast.jet.impl.statemachine.jobmanager.processors.ExecutionErrorProcessor;
import com.hazelcast.jet.impl.statemachine.jobmanager.processors.ExecutionPlanBuilderProcessor;
import com.hazelcast.jet.impl.statemachine.jobmanager.processors.InterruptJobProcessor;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.ExecutionCompletedRequest;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.ExecutionErrorRequest;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.ExecutionInterruptedRequest;
import com.hazelcast.jet.impl.util.BasicCompletableFuture;
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

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:methodcount"})
public class JobManager implements StateMachineRequestProcessor<JobManagerEvent> {

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final InterruptedException JOB_INTERRUPTED_EXCEPTION =
            new InterruptedException("Job has been interrupted");

    private final List<VertexRunner> runners = new CopyOnWriteArrayList<>();
    private final Map<Integer, VertexRunner> runnersMap = new ConcurrentHashMap<>();
    private final Map<Vertex, VertexRunner> vertex2runner = new ConcurrentHashMap<>();

    private final AtomicInteger runnerCounter = new AtomicInteger(0);

    private final AtomicInteger networkTaskCounter = new AtomicInteger(0);
    private final AtomicReference<BasicCompletableFuture<Object>> executionMailBox =
            new AtomicReference<>(null);
    private final AtomicReference<BasicCompletableFuture<Object>> interruptionFutureHolder =
            new AtomicReference<>(null);

    private final ILogger logger;
    private final DiscoveryService discoveryService;
    private final JobContext jobContext;
    private final JobManagerStateMachine stateMachine;
    private volatile boolean interrupted;
    private volatile Throwable interruptionError;
    private volatile DAG dag;
    private final byte[] jobNameBytes;

    public JobManager(JobContext jobContext, DiscoveryService discoveryService) {
        this.stateMachine = new JobManagerStateMachine(jobContext.getName(), this, jobContext);
        this.jobContext = jobContext;
        this.discoveryService = discoveryService;
        jobNameBytes = jobContext.getNodeEngine().getSerializationService().toData(jobContext.getName()).toByteArray();
        logger = jobContext.getNodeEngine().getLogger(JobManager.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processRequest(JobManagerEvent event, Object payload) throws Exception {
        VertexRunnerPayloadProcessor processor = getProcessor(event, this);

        if (processor != null) {
            processor.process(payload);
        }
    }

    /**
     * Handle event when some runner has been completed;
     */
    public void handleRunnerCompleted() {
        if (runnerCounter.incrementAndGet() >= runners.size()) {
            if (getNetworkTaskCount() > 0) {
                List<Task> networkTasks = getJobContext().getExecutorContext().getNetworkTasks();
                networkTasks.forEach(Task::finalizeTask);
            } else {
                notifyCompletionFinished();
            }
        }
    }

    /**
     * Handle event when some runner has been interrupted;
     *
     * @param error corresponding error;
     */
    public void handleRunnerInterrupted(Throwable error) {
        logger.info("Vertex runner has been interrupted :  " + error);
        interrupted = true;
        interruptionError = error;
        handleRunnerCompleted();
    }

    private void notifyCompletionFinished() {
        try {
            if (interrupted) {
                notifyInterrupted(interruptionError);
            } else {
                try {
                    handleRequest(new ExecutionCompletedRequest()).get(
                            getJobContext().getJobConfig().getSecondsToAwait(),
                            TimeUnit.SECONDS
                    );
                } finally {
                    addToExecutionMailBox(true);
                }
            }
        } catch (Throwable e) {
            throw unchecked(e);
        }
    }

    private int getNetworkTaskCount() {
        return getJobContext().getExecutorContext().getNetworkTasks().size();
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private void notifyInterrupted(Throwable error) throws Exception {
        logger.info("notify interrupted " + error);

        Throwable interruptionError =
                error == null ? JOB_INTERRUPTED_EXCEPTION : error;

        try {
            try {
                handleRequest(new ExecutionInterruptedRequest()).get(
                        getJobContext().getJobConfig().getSecondsToAwait(),
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
     * Register job execution;
     */
    public void registerExecution() {
        interrupted = false;
        interruptionError = null;
        runnerCounter.set(0);
        networkTaskCounter.set(0);
        executionMailBox.set(new BasicCompletableFuture<>(jobContext.getNodeEngine(), logger));
        interruptionFutureHolder.set(null);
    }

    /**
     * Register job interruption;
     */
    public void registerInterruption() {
        interruptionFutureHolder.set(new BasicCompletableFuture<>(jobContext.getNodeEngine(), logger));
    }

    /**
     * @return mailBox which is used to signal that job's;
     * execution has been completed;
     */
    public ICompletableFuture<Object> getExecutionMailBox() {
        return executionMailBox.get();
    }

    /**
     * @return mailBox which is used to signal that job's;
     * interruption has been completed;
     */
    public ICompletableFuture<Object> getInterruptionMailBox() {
        return interruptionFutureHolder.get();
    }

    /**
     * @return list of runners;
     */
    public List<VertexRunner> runners() {
        return Collections.unmodifiableList(runners);
    }

    private void addToExecutionMailBox(Object object) {
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
        for (MemberImpl member : jobContext.getNodeEngine().getClusterService().getMemberImpls()) {
            if (member.localMember()) {
                notifyRunners(reason);
            } else {
                notifyClusterMembers(reason, member);
            }
        }
    }

    private void notifyClusterMembers(Object reason, MemberImpl member) {
        JetPacket jetPacket = new JetPacket(
                jobNameBytes,
                toBytes(reason)
        );

        jetPacket.setHeader(JetPacket.HEADER_JET_EXECUTION_ERROR);
        Address jetAddress = getJobContext().getHzToJetAddressMapping().get(member.getAddress());
        discoveryService.getSocketWriters().get(jetAddress).sendServicePacket(jetPacket);
    }

    /**
     * Notify all job's runners with some signal;
     *
     * @param reason signal object;
     */
    public void notifyRunners(Object reason) {
        Throwable error = getError(reason);
        handleRequest(new ExecutionErrorRequest(error));
    }

    private Throwable getError(Object reason) {
        Throwable error;

        if (reason instanceof JetPacket) {
            JetPacket packet = (JetPacket) reason;
            Address initiator = packet.getRemoteMember();

            error = packet.toByteArray() == null
                    ? new JobException(initiator) : toException(initiator, (JetPacket) reason);
        } else if (reason instanceof Address) {
            error = new JobException((Address) reason);
        } else if (reason instanceof Throwable) {
            error = (Throwable) reason;
        } else {
            error = new JobException(reason, jobContext.getNodeEngine().getLocalMember().getAddress());
        }

        return error;
    }

    private Throwable toException(Address initiator, JetPacket packet) {
        Object object = jobContext.getNodeEngine().getSerializationService().toObject(new HeapData(packet.toByteArray()));

        if (object instanceof Throwable) {
            return new CombinedJetException(Arrays.asList((Throwable) object, new JobException(initiator)));
        } else if (object instanceof JetPacket) {
            return new JobException(initiator, (JetPacket) object);
        } else {
            return new JobException(object, initiator);
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

        return ((InternalSerializationService) jobContext.getNodeEngine().getSerializationService()).toBytes(reason);
    }

    /**
     * Register shuffling receiver for the corresponding task and address;
     *
     * @param taskID          corresponding taskID
     * @param vertexManagerId vertex manager id;
     * @param address         corresponding address;
     * @param receiver        registered receiver;
     */
    public void registerShufflingReceiver(int taskID,
                                          int vertexManagerId,
                                          Address address,
                                          ShufflingReceiver receiver) {
        VertexRunner vertexRunner = runnersMap.get(vertexManagerId);
        VertexTask vertexTask = vertexRunner.getVertexMap().get(taskID);
        vertexTask.registerShufflingReceiver(address, receiver);
        discoveryService.getSocketReaders().get(address).registerConsumer(receiver.getRingbuffer());
    }

    /**
     * Register shuffling receiver for the corresponding task and address;
     *
     * @param taskID          corresponding taskID
     * @param vertexManagerId vertex manager id
     * @param address         corresponding address;
     * @param sender          registered sender;
     */
    public void registerShufflingSender(int taskID,
                                        int vertexManagerId,
                                        Address address,
                                        ShufflingSender sender) {
        VertexRunner vertexRunner = runnersMap.get(vertexManagerId);
        VertexTask vertexTask = vertexRunner.getVertexMap().get(taskID);
        vertexTask.registerShufflingSender(address, sender);
        discoveryService.getSocketWriters().get(address).registerProducer(sender.getRingbuffer());
    }

    /**
     * @return map with runners;
     */
    public Map<Integer, VertexRunner> getRunnersMap() {
        return runnersMap;
    }

    /**
     * @return dag of the job;
     */
    public DAG getDag() {
        return dag;
    }

    /**
     * Set up dag for the corresponding job;
     *
     * @param dag corresponding dag;
     */
    public void setDag(DAG dag) {
        this.dag = dag;
    }

    /**
     * @param vertex the vertex to get the vertex runner
     * @return runner for the corresponding vertex;
     */
    public VertexRunner getRunnerByVertex(Vertex vertex) {
        return vertex2runner.get(vertex);
    }

    public JobManagerStateMachine getStateMachine() {
        return stateMachine;
    }

    /**
     * Register runner for the specified vertex;
     *
     * @param vertex corresponding vertex;
     * @param runner corresponding runner;
     */
    public void registerRunner(Vertex vertex, VertexRunner runner) {
        runners.add(runner);
        vertex2runner.put(vertex, runner);
        runnersMap.put(runner.getId(), runner);
    }

    private void wakeUpExecutor() {
        getJobContext().getExecutorContext().getJobManagerStateMachineExecutor().wakeUp();
    }

    private static VertexRunnerPayloadProcessor getProcessor(JobManagerEvent event, JobManager jobManager) {
        switch (event) {
            case SUBMIT_DAG:
                return new ExecutionPlanBuilderProcessor(jobManager);
            case EXECUTE:
                return new ExecuteJobProcessor(jobManager);
            case INTERRUPT_EXECUTION:
                return new InterruptJobProcessor(jobManager);
            case EXECUTION_ERROR:
                return new ExecutionErrorProcessor(jobManager);
            case EXECUTION_COMPLETED:
                return new ExecutionCompletedProcessor();
            case FINALIZE:
                return new DestroyJobProcessor(jobManager);
            default:
                return null;
        }
    }

    /**
     * Handle's runner request with state-machine's input event
     *
     * @param request corresponding request
     * @param <P>     type of request payload
     * @return awaiting future
     */
    public <P> ICompletableFuture<JobManagerResponse> handleRequest(StateMachineRequest<JobManagerEvent, P> request) {
        try {
            return stateMachine.handleRequest(request);
        } finally {
            wakeUpExecutor();
        }
    }

    /**
     * @return JET-job context
     */
    public JobContext getJobContext() {
        return jobContext;
    }

}
