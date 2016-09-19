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
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Sink;
import com.hazelcast.jet.Source;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.ringbuffer.CompositeRingbuffer;
import com.hazelcast.jet.impl.ringbuffer.Ringbuffer;
import com.hazelcast.jet.impl.runtime.events.TaskEventHandlerFactory;
import com.hazelcast.jet.impl.runtime.task.TaskContextImpl;
import com.hazelcast.jet.impl.runtime.task.TaskEvent;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.impl.statemachine.StateMachine;
import com.hazelcast.jet.impl.statemachine.StateMachineEventHandler;
import com.hazelcast.jet.impl.statemachine.StateMachineRequest;
import com.hazelcast.jet.impl.statemachine.runner.VertexRunnerStateMachine;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerFinalizedRequest;
import com.hazelcast.jet.runtime.Consumer;
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
import static java.util.Arrays.asList;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class VertexRunner implements StateMachineEventHandler<VertexRunnerEvent> {

    private final Vertex vertex;
    private final Map<Integer, VertexTask> vertexTaskMap = new ConcurrentHashMap<>();
    private final VertexTask[] vertexTasks;
    private final TaskEventHandlerFactory taskEventHandlerFactory;
    private final int id;
    private final JobContext jobContext;
    private final StateMachine<VertexRunnerEvent, VertexRunnerState, VertexRunnerResponse> stateMachine;

    public VertexRunner(int id, Vertex vertex, JobContext jobContext) {
        this.stateMachine = new VertexRunnerStateMachine(vertex.getName(), this, jobContext);
        this.jobContext = jobContext;
        this.id = id;
        this.vertex = vertex;
        this.vertexTasks = new VertexTask[vertex.getParallelism()];
        this.taskEventHandlerFactory = new TaskEventHandlerFactory(this);

        buildTasks();
        buildSources();
        buildSinks();
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

    public void handleTaskEvent(VertexTask vertexTask, TaskEvent event) {
        handleTaskEvent(vertexTask, event, null);
    }

    public void handleTaskEvent(VertexTask vertexTask, TaskEvent event, Throwable error) {
        taskEventHandlerFactory.getEventProcessor(event).handle(vertexTask, event, error);
    }

    public VertexTask[] getVertexTasks() {
        return vertexTasks;
    }

    public Map<Integer, VertexTask> getVertexMap() {
        return vertexTaskMap;
    }

    public Vertex getVertex() {
        return vertex;
    }

    public void start() {
        for (VertexTask vertexTask : vertexTasks) {
            vertexTask.start();
        }
    }

    @Override
    public void handleEvent(VertexRunnerEvent event, Object payload) {
        switch (event) {
            case START:
                start();
                return;
            case EXECUTION_COMPLETED:
                complete();
                jobContext.getJobManager().handleRunnerCompleted();
                return;
            case INTERRUPT:
                interrupt((Throwable) payload);
                return;
            case INTERRUPTED:
                jobContext.getJobManager().handleRunnerInterrupted((Throwable) payload);
                return;
            default:
        }
    }

    /**
     * Destroys the runners.
     */
    public void destroy() throws Exception {
        int timeout = getJobContext().getJobConfig().getSecondsToAwait();
        handleRequest(new VertexRunnerFinalizedRequest(this)).get(timeout, TimeUnit.SECONDS);
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

    public void complete() {
        for (VertexTask task : vertexTasks) {
            task.complete();
        }
    }

    /**
     * Connect this vertex with another one by creating the necessary ringbuffers
     * between the vertices
     */
    public VertexRunner connect(VertexRunner target, Edge edge) {
        for (VertexTask sourceTask : getVertexTasks()) {
            CompositeRingbuffer consumer = createRingbuffers(target, edge);
            sourceTask.addConsumer(consumer);

            VertexTask[] vertexTasks = target.getVertexTasks();
            for (int i = 0, vertexTasksLength = vertexTasks.length; i < vertexTasksLength; i++) {
                VertexTask targetTask = vertexTasks[i];
                targetTask.addProducer(consumer.getRingbuffers()[i]);
            }
        }
        return target;
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
            getJobContext().getExecutorContext().getVertexManagerStateMachineExecutor().wakeUp();
        }
    }

    public JobContext getJobContext() {
        return jobContext;
    }

    public int getId() {
        return id;
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

    private Processor createProcessor(String className, Object... args) {
        ClassLoader classLoader = jobContext.getDeploymentStorage().getClassLoader();
        try {
            Constructor<Processor> resultConstructor = findConstructor(classLoader, className, args);
            return resultConstructor.newInstance(args);
        } catch (Exception e) {
            throw unchecked(e);
        }
    }

    private CompositeRingbuffer createRingbuffers(VertexRunner target, Edge edge) {
        JobContext jobContext = target.getJobContext();
        List<Ringbuffer> consumers = new ArrayList<>(target.getVertexTasks().length);
        for (int i = 0; i < target.getVertexTasks().length; i++) {
            Ringbuffer ringbuffer = new Ringbuffer(getVertex().getName(),
                    jobContext, edge);
            consumers.add(ringbuffer);
        }
        return new CompositeRingbuffer(jobContext, consumers, edge);
    }

    private void buildSinks() {
        for (Sink sink : getVertex().getSinks()) {
            if (sink.isPartitioned()) {
                for (VertexTask vertexTask : getVertexTasks()) {
                    vertexTask.addConsumers(asList(sink.getConsumers(jobContext, vertex)));
                }
            } else {
                final Consumer[] consumers = sink.getConsumers(jobContext, vertex);
                int i = 0;
                for (VertexTask vertexTask : getVertexTasks()) {
                    vertexTask.addConsumer(consumers[i++]);
                }
            }
        }
    }

    private void buildSources() {
        List<Producer> producers = new ArrayList<>();
        for (Source source : getVertex().getSources()) {
            producers.addAll(asList(source.getProducers(jobContext, vertex)));
        }
        for (int i = 0; i < producers.size(); i++) {
            Producer producer = producers.get(i);
            int taskId = i % vertexTasks.length;
            vertexTasks[taskId].addProducer(producer);
        }
    }

    @Override
    public String toString() {
        return "VertexRunner{name=" + vertex.getName() + "}";
    }
}
