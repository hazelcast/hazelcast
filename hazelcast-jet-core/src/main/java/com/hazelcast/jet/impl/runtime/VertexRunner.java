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
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Sink;
import com.hazelcast.jet.Source;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.events.EventProcessorFactory;
import com.hazelcast.jet.impl.runtime.runner.VertexRunnerEvent;
import com.hazelcast.jet.impl.runtime.runner.VertexRunnerResponse;
import com.hazelcast.jet.impl.runtime.runner.VertexRunnerState;
import com.hazelcast.jet.impl.runtime.task.TaskContextImpl;
import com.hazelcast.jet.impl.runtime.task.TaskEvent;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.impl.statemachine.StateMachine;
import com.hazelcast.jet.impl.statemachine.StateMachineRequest;
import com.hazelcast.jet.impl.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.impl.statemachine.runner.VertexRunnerStateMachine;
import com.hazelcast.jet.impl.statemachine.runner.processors.VertexRunnerPayloadFactory;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerFinalizedRequest;
import com.hazelcast.jet.runtime.DataWriter;
import com.hazelcast.jet.runtime.Producer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;
import static java.util.stream.Collectors.toList;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class VertexRunner implements StateMachineRequestProcessor<VertexRunnerEvent> {

    private final Vertex vertex;
    private final Map<Integer, VertexTask> vertexTaskMap = new ConcurrentHashMap<>();
    private final int awaitSecondsTimeOut;
    private final VertexTask[] vertexTasks;
    private final List<DataChannel> inputChannels = new ArrayList<>();
    private final List<DataChannel> outputChannels = new ArrayList<>();
    private final List<Producer> sourcesProducers = new ArrayList<>();
    private final EventProcessorFactory eventProcessorFactory;

    private final int id;
    private final JobContext jobContext;
    private final StateMachine<VertexRunnerEvent, VertexRunnerState, VertexRunnerResponse> stateMachine;

    public VertexRunner(int id, Vertex vertex, JobContext jobContext) {
        this.stateMachine = new VertexRunnerStateMachine(vertex.getName(), this, jobContext);
        this.jobContext = jobContext;
        this.id = id;
        this.vertex = vertex;
        this.vertexTasks = new VertexTask[vertex.getParallelism()];
        this.awaitSecondsTimeOut = getJobContext().getJobConfig().getSecondsToAwait();
        this.eventProcessorFactory = new EventProcessorFactory(this);

        buildTasks();
        buildSources();
        buildSinks();
    }

    private void buildTasks() {
        ClassLoader classLoader = getJobContext().getDeploymentStorage().getClassLoader();
        for (int taskIndex = 0; taskIndex < vertexTasks.length; taskIndex++) {
            Processor processor = createProcessor(vertex.getProcessorClass(), vertex.getProcessorArgs());
            vertexTasks[taskIndex] = new VertexTask(this, getVertex(),
                    new TaskContextImpl(vertex, jobContext, processor, taskIndex));
            getJobContext().getExecutorContext().getProcessingTasks().add(vertexTasks[taskIndex]);
            vertexTasks[taskIndex].setThreadContextClassLoader(classLoader);
            vertexTaskMap.put(taskIndex, vertexTasks[taskIndex]);
        }
    }

    @SuppressWarnings("unchecked")
    private Processor createProcessor(String className, Object... args) {
        ClassLoader classLoader = jobContext.getDeploymentStorage().getClassLoader();
        try {
            Constructor<Processor> resultConstructor = findConstructor(classLoader, className, args);
            return resultConstructor.newInstance(args);
        } catch (Exception e) {
            throw unchecked(e);
        }
    }

    private static Constructor<Processor> findConstructor(ClassLoader classLoader, String className, Object[] args)
            throws ClassNotFoundException {
        Class<Processor> clazz = (Class<Processor>) Class.forName(className, true, classLoader);
        int i = 0;
        Class[] argsClasses = new Class[args.length];
        for (Object obj : args) {
            if (obj != null) {
                argsClasses[i++] = obj.getClass();
            }
        }
        for (Constructor constructor : clazz.getConstructors()) {
            if (constructor.getParameterTypes().length == argsClasses.length) {
                boolean valid = true;
                Class[] parameterTypes = constructor.getParameterTypes();
                for (int idx = 0; idx < argsClasses.length; idx++) {
                    Class argsClass = argsClasses[idx];
                    if ((argsClass != null) && !parameterTypes[idx].isAssignableFrom(argsClass)) {
                        valid = false;
                        break;
                    }
                }
                if (valid) {
                    return (Constructor<Processor>) constructor;
                }
            }
        }
        throw new IllegalStateException(
                "No constructor with arguments" + Arrays.toString(argsClasses) + " className=" + className);
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
