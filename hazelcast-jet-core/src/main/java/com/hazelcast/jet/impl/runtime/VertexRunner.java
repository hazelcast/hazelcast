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
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.sink.Sink;
import com.hazelcast.jet.dag.source.Source;
import com.hazelcast.jet.data.DataWriter;
import com.hazelcast.jet.impl.actor.Producer;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.events.DefaultEventProcessorFactory;
import com.hazelcast.jet.impl.runtime.events.EventProcessorFactory;
import com.hazelcast.jet.impl.runtime.runner.VertexRunnerEvent;
import com.hazelcast.jet.impl.runtime.runner.VertexRunnerResponse;
import com.hazelcast.jet.impl.runtime.runner.VertexRunnerState;
import com.hazelcast.jet.impl.runtime.task.DefaultTaskContext;
import com.hazelcast.jet.impl.runtime.task.TaskEvent;
import com.hazelcast.jet.impl.runtime.task.TaskProcessorFactory;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.impl.runtime.task.processors.factory.DefaultTaskProcessorFactory;
import com.hazelcast.jet.impl.runtime.task.processors.factory.ShuffledTaskProcessorFactory;
import com.hazelcast.jet.impl.statemachine.StateMachine;
import com.hazelcast.jet.impl.statemachine.StateMachineRequest;
import com.hazelcast.jet.impl.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.impl.statemachine.runner.VertexRunnerStateMachine;
import com.hazelcast.jet.impl.statemachine.runner.processors.VertexRunnerPayloadFactory;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerFinalizedRequest;
import com.hazelcast.jet.processor.Processor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class VertexRunner implements StateMachineRequestProcessor<VertexRunnerEvent> {

    private final Vertex vertex;

    private final Map<Integer, VertexTask> vertexTaskMap = new ConcurrentHashMap<>();

    private final int parallelism;

    private final int awaitSecondsTimeOut;

    private final VertexTask[] vertexTasks;

    private final List<DataChannel> inputChannels;

    private final List<DataChannel> outputChannels;

    private final List<Producer> sourcesProducers;

    private final TaskProcessorFactory taskProcessorFactory;

    private final EventProcessorFactory eventProcessorFactory;

    private final Supplier<Processor> processorSupplier;

    private final AtomicInteger taskIdGenerator = new AtomicInteger(0);
    private final int id;
    private final JobContext jobContext;
    private final StateMachine<VertexRunnerEvent, VertexRunnerState, VertexRunnerResponse> stateMachine;

    public VertexRunner(Vertex vertex, Supplier<Processor> processorSupplier,
                        JobContext jobContext) {
        this.stateMachine = new VertexRunnerStateMachine(vertex.getName(), this, jobContext);
        this.jobContext = jobContext;
        this.id = jobContext.getVertexRunnerIdGenerator().incrementAndGet();
        this.vertex = vertex;
        this.inputChannels = new ArrayList<>();
        this.outputChannels = new ArrayList<>();
        this.taskProcessorFactory = isShuffled(vertex) ? new ShuffledTaskProcessorFactory()
                : new DefaultTaskProcessorFactory();
        this.parallelism = vertex.getParallelism();
        this.sourcesProducers = new ArrayList<>();
        this.processorSupplier = processorSupplier;
        this.vertexTasks = new VertexTask[this.parallelism];
        this.awaitSecondsTimeOut = getJobContext().getJobConfig().getSecondsToAwait();
        AtomicInteger readyForFinalizationTasksCounter = new AtomicInteger(0);
        readyForFinalizationTasksCounter.set(this.parallelism);
        AtomicInteger completedTasks = new AtomicInteger(0);
        AtomicInteger interruptedTasks = new AtomicInteger(0);
        this.eventProcessorFactory = new DefaultEventProcessorFactory(completedTasks, interruptedTasks,
                readyForFinalizationTasksCounter, this.vertexTasks, this);
        buildTasks();
        buildSources();
        buildSinks();
    }

    private void buildTasks() {
        VertexTask[] vertexTasks = getVertexTasks();
        ClassLoader classLoader = getJobContext().getDeploymentStorage().getClassLoader();
        for (int taskIndex = 0; taskIndex < this.parallelism; taskIndex++) {
            int taskID = taskIdGenerator.incrementAndGet();
            vertexTasks[taskIndex] = new VertexTask(this, getVertex(), taskProcessorFactory,
                    taskID, new DefaultTaskContext(parallelism, taskIndex, getJobContext()));
            getJobContext().getExecutorContext().getProcessingTasks().add(vertexTasks[taskIndex]);
            vertexTasks[taskIndex].setThreadContextClassLoader(classLoader);
            vertexTaskMap.put(taskID, vertexTasks[taskIndex]);
        }
    }

    /**
     * Handles task's event;
     *
     * @param vertexTask - corresponding vertex task;
     * @param event      - task's event;
     */
    public void handleTaskEvent(VertexTask vertexTask, TaskEvent event) {
        handleTaskEvent(vertexTask, event, null);
    }

    /**
     * Handles task's error-event;
     *
     * @param vertexTask - corresponding vertex task;
     * @param event      - task's event;
     * @param error      - corresponding error;
     */
    public void handleTaskEvent(VertexTask vertexTask, TaskEvent event, Throwable error) {
        this.eventProcessorFactory.getEventProcessor(event).process(vertexTask, event, error);
    }

    /**
     * @return - user-level processor supplier;
     */
    public final Supplier<Processor> getProcessorSupplier() {
        return processorSupplier;
    }

    /**
     * @return - tasks of the vertex;
     */
    public VertexTask[] getVertexTasks() {
        return vertexTasks;
    }

    /**
     * TaskID -&gt; VertexTask;
     *
     * @return - cache of vertex tasks;
     */
    public Map<Integer, VertexTask> getVertexMap() {
        return vertexTaskMap;
    }

    /**
     * @return - vertex for the corresponding vertex runner;
     */
    public Vertex getVertex() {
        return vertex;
    }

    /**
     * @return - list of the input channels;
     */
    public List<DataChannel> getInputChannels() {
        return inputChannels;
    }

    /**
     * @return - list of the output channels;
     */
    public List<DataChannel> getOutputChannels() {
        return outputChannels;
    }

    /**
     * Adds input channel for vertex runner.
     */
    public void addInputChannel(DataChannel channel) {
        inputChannels.add(channel);
    }

    /**
     * Adds output channel for vertex runner.
     */
    public void addOutputChannel(DataChannel channel) {
        outputChannels.add(channel);
    }

    /**
     * Starts execution of runners
     */
    public void start() {
        int taskCount = getVertexTasks().length;
        if (taskCount == 0) {
            throw new IllegalStateException("No tasks found for the vertex runner!");
        }
        List<Producer> producers = new ArrayList<>(sourcesProducers);
        List<Producer>[] tasksProducers = new List[taskCount];
        for (int taskIdx = 0; taskIdx < getVertexTasks().length; taskIdx++) {
            tasksProducers[taskIdx] = new ArrayList<>();
        }
        int taskId = 0;
        for (Producer producer : producers) {
            tasksProducers[taskId].add(producer);
            taskId = (taskId + 1) % taskCount;
        }
        for (int taskIdx = 0; taskIdx < getVertexTasks().length; taskIdx++) {
            startTask(tasksProducers[taskIdx], taskIdx);
        }
    }

    private void startTask(List<Producer> producers, int taskIdx) {
        for (DataChannel channel : getInputChannels()) {
            producers.addAll(channel.getActors()
                    .stream()
                    .map(actor -> actor.getParties()[taskIdx])
                    .collect(toList()));
        }
        getVertexTasks()[taskIdx].start(producers);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processRequest(VertexRunnerEvent event, Object payload) throws Exception {
        VertexRunnerPayloadProcessor processor = VertexRunnerPayloadFactory.getProcessor(event, this);
        if (processor != null) {
            processor.process(payload);
        }
    }

    /**
     * Destroys the runners.
     */
    public void destroy() throws Exception {
        handleRequest(new VertexRunnerFinalizedRequest(this)).get(this.awaitSecondsTimeOut, TimeUnit.SECONDS);
    }

    /**
     * Interrupts the execution of runners.
     *
     * @param error the error that's causing the interruption
     */
    public void interrupt(Throwable error) {
        for (VertexTask task : vertexTasks) {
            task.interrupt(error);
        }
    }

    protected void wakeUpExecutor() {
        getJobContext().getExecutorContext().getVertexManagerStateMachineExecutor().wakeUp();
    }

    private boolean isShuffled(Vertex vertex) {
        return (hasOutputShuffler(vertex) && clusterHasMultipleMembers()) || hasSink(vertex);
    }

    private boolean clusterHasMultipleMembers() {
        return getJobContext().getNodeEngine().getClusterService().getMembers().size() > 1;
    }

    private boolean hasOutputShuffler(Vertex vertex) {
        return vertex.hasOutputShuffler();
    }

    private boolean hasSink(Vertex vertex) {
        return vertex.getSinks().size() > 0;
    }

    private void buildSinks() {
        List<Sink> sinks = getVertex().getSinks();
        for (Sink sink : sinks) {
            if (sink.isPartitioned()) {
                for (VertexTask vertexTask : getVertexTasks()) {
                    List<DataWriter> writers = getDataWriters(sink);
                    vertexTask.registerSinkWriters(writers);
                }
            } else {
                List<DataWriter> writers = getDataWriters(sink);
                int i = 0;

                for (VertexTask vertexTask : getVertexTasks()) {
                    List<DataWriter> sinkWriters = new ArrayList<>(sinks.size());
                    if (writers.size() >= i - 1) {
                        sinkWriters.add(writers.get(i++));
                        vertexTask.registerSinkWriters(sinkWriters);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    private void buildSources() {
        if (getVertex().getSources().size() > 0) {
            for (Source source : getVertex().getSources()) {
                List<Producer> readers = getDataReaders(source);
                sourcesProducers.addAll(readers);
            }
        }
    }

    private List<Producer> getDataReaders(Source source) {
        return Arrays.asList(source.getProducers(jobContext, getVertex()));
    }

    private List<DataWriter> getDataWriters(Sink sink) {
        return Arrays.asList(sink.getWriters(jobContext));
    }

    /**
     * Handle's vertex runner request with state-machine's input event
     *
     * @param request corresponding request
     * @param <P>     type of request payload
     * @return awaiting future
     */
    public <P> ICompletableFuture<VertexRunnerResponse> handleRequest(
            StateMachineRequest<VertexRunnerEvent, P> request) {
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

    /**
     * @return vertex runner's identifier
     */
    public int getId() {
        return id;
    }
}
