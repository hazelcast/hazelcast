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

package com.hazelcast.jet.impl.runtime.task;


import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.DataWriter;
import com.hazelcast.jet.executor.TaskContext;
import com.hazelcast.jet.impl.actor.Actor;
import com.hazelcast.jet.impl.actor.ComposedActor;
import com.hazelcast.jet.impl.actor.Consumer;
import com.hazelcast.jet.impl.actor.Producer;
import com.hazelcast.jet.impl.actor.RingbufferActor;
import com.hazelcast.jet.impl.actor.shuffling.ShufflingActor;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.impl.runtime.DataChannel;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.util.BooleanHolder;
import com.hazelcast.jet.processor.Processor;
import com.hazelcast.jet.processor.ProcessorContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Represents actual vertex execution
 */
@SuppressWarnings("checkstyle:methodcount")
public class VertexTask extends Task {

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final InterruptedException INTERRUPTED_EXCEPTION =
            new InterruptedException("Execution has been interrupted");

    private final ILogger logger;

    private final int taskID;

    private final Vertex vertex;

    private final NodeEngine nodeEngine;

    private final VertexRunner runner;
    private final Processor processor;
    private final JobContext jobContext;
    private final TaskProcessorFactory taskProcessorFactory;
    private final AtomicBoolean interrupted = new AtomicBoolean(false);
    private final AtomicInteger activeProducersCounter = new AtomicInteger(0);
    private final AtomicInteger activeReceiversCounter = new AtomicInteger(0);
    private final AtomicInteger finalizedReceiversCounter = new AtomicInteger(0);
    private final Collection<Consumer> consumers = new CopyOnWriteArrayList<>();
    private final Collection<Producer> producers = new CopyOnWriteArrayList<>();
    private final Map<Address, ShufflingReceiver> shufflingReceivers = new ConcurrentHashMap<>();
    private final Map<Address, ShufflingSender> shufflingSenders = new ConcurrentHashMap<>();
    private final TaskContext taskContext;
    private final ProcessorContext processorContext;
    private volatile TaskProcessor taskProcessor;
    private volatile boolean sendersClosed;
    private volatile ShufflingSender[] sendersArray;
    private volatile boolean sendersFlushed;
    private boolean producersClosed;
    private boolean receiversClosed;
    private boolean isFinalizationNotified;
    private volatile boolean finalizationStarted;
    private volatile Throwable error;

    public VertexTask(VertexRunner vertexRunner,
                      Vertex vertex,
                      TaskProcessorFactory taskProcessorFactory,
                      int taskID,
                      TaskContext taskContext) {
        this.taskID = taskID;
        this.vertex = vertex;
        this.runner = vertexRunner;
        this.taskContext = taskContext;
        this.taskProcessorFactory = taskProcessorFactory;
        jobContext = vertexRunner.getJobContext();
        nodeEngine = jobContext.getNodeEngine();
        Supplier<Processor> processorFactory = vertexRunner.getProcessorSupplier();
        processor = processorFactory == null ? null : processorFactory.get();
        processorContext = new ProcessorContext(vertex, jobContext, taskContext);
        logger = nodeEngine.getLogger(getClass());
    }

    /**
     * Start tasks' execution
     * Initialize initial state of the task
     *
     * @param producers - list of the input producers
     */
    public void start(List<? extends Producer> producers) {
        if (producers != null && !producers.isEmpty()) {
            for (Producer producer : producers) {
                this.producers.add(producer);
                producer.registerCompletionHandler(this::handleProducerCompleted);
            }
        }
        onStart();
    }

    /**
     * Interrupts tasks execution
     *
     * @param error - the reason of the interruption
     */
    public void interrupt(Throwable error) {
        if (interrupted.compareAndSet(false, true)) {
            error = error;
        }
    }

    /**
     * Performs registration of sink writers
     *
     * @param sinkWriters - list of the input sink writers
     */
    public void registerSinkWriters(List<DataWriter> sinkWriters) {
        consumers.addAll(sinkWriters);
    }

    /**
     * @param channel       - data channel for corresponding edge
     * @param edge          - corresponding edge
     * @param vertexRunner - source vertex manager of the channel
     * @return - composed actor with actors of channel
     */
    public ComposedActor registerOutputChannel(DataChannel channel, Edge edge, VertexRunner vertexRunner) {
        List<Actor> actors = new ArrayList<Actor>(vertexRunner.getVertexTasks().length);

        for (int i = 0; i < vertexRunner.getVertexTasks().length; i++) {
            Actor actor = new RingbufferActor(nodeEngine, jobContext, this, vertex, edge);

            if (channel.isShuffled()) {
                //output
                actor = new ShufflingActor(actor, nodeEngine);
            }
            actors.add(actor);
        }

        ComposedActor composed = new ComposedActor(this, actors, vertex, edge, jobContext);
        consumers.add(composed);

        return composed;
    }

    /**
     * Handled on input producer's completion
     *
     * @param producer - finished input producer
     */
    public void handleProducerCompleted(Producer producer) {
        activeProducersCounter.decrementAndGet();
    }

    /**
     * Register shuffling receiver for the corresponding node with address member
     *
     * @param address  - member's address
     * @param receiver - corresponding shuffling receiver
     */
    public void registerShufflingReceiver(Address address, ShufflingReceiver receiver) {
        shufflingReceivers.put(address, receiver);
        receiver.registerCompletionHandler(producer -> activeReceiversCounter.decrementAndGet());
    }

    /**
     * @return - task context
     */
    public TaskContext getTaskContext() {
        return taskContext;
    }

    /**
     * @param endPoint - jet-Address of the corresponding shuffling-receiver
     * @return - corresponding shuffling-receiver
     */
    public ShufflingReceiver getShufflingReceiver(Address endPoint) {
        return shufflingReceivers.get(endPoint);
    }

    /**
     * @return - corresponding DAG's vertex
     */
    public Vertex getVertex() {
        return processorContext.getVertex();
    }

    /**
     * Start finalization of the task
     */
    public void startFinalization() {
        finalizationStarted = true;
    }

    /**
     * Register shuffling sender for the corresponding node with address member
     *
     * @param address - member's address
     * @param sender  - corresponding shuffling sender
     */
    public void registerShufflingSender(Address address, ShufflingSender sender) {
        shufflingSenders.put(address, sender);
    }

    /**
     * Init task, perform initialization actions before task being executed
     * The strict rule is that this method will be executed synchronously on
     * all nodes in cluster before any real task's  execution
     */
    public void init() {
        error = null;
        taskProcessor.onOpen();
        interrupted.set(false);
        sendersClosed = false;
        receiversClosed = false;
        sendersFlushed = false;
        producersClosed = false;
        activeProducersCounter.set(producers.size());
        activeReceiversCounter.set(shufflingReceivers.values().size());
        finalizedReceiversCounter.set(shufflingReceivers.values().size());
    }

    /***
     * Will be invoked immediately before task was submitted into the executor,
     * strictly from executor-thread
     */
    public void beforeProcessing() {
        try {
            processor.before(processorContext);
        } catch (Throwable error) {
            afterProcessing();
            handleProcessingError(error);
        }
    }

    /**
     * Execute next iteration of task
     *
     * @param didWorkHolder flag to set to indicate that the task did something useful
     * @return - true - if task should be executed again, false if task should be removed from executor
     * @throws Exception if any exception
     */
    public boolean execute(BooleanHolder didWorkHolder) {
        TaskProcessor processor = taskProcessor;
        boolean classLoaderChanged = false;
        ClassLoader classLoader = null;

        if (contextClassLoader != null) {
            classLoader = Thread.currentThread().getContextClassLoader();
            if (contextClassLoader != classLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
                classLoaderChanged = true;
            }
        }

        try {
            if (interrupted.get()) {
                try {
                    onInterrupt(processor);
                } finally {
                    runner.handleTaskEvent(this, TaskEvent.TASK_EXECUTION_COMPLETED, getError());
                }
                return false;
            }

            boolean result;
            Throwable error = null;

            try {
                result = process(didWorkHolder, processor);
            } catch (Throwable e) {
                result = false;
                didWorkHolder.set(false);
                error = e;
            }

            handleResult(result, error);
            return result;
        } finally {
            if (classLoaderChanged) {
                Thread.currentThread().setContextClassLoader(classLoader);
            }
        }
    }

    private Throwable getError() {
        return error != null ? error : INTERRUPTED_EXCEPTION;
    }

    private void onInterrupt(TaskProcessor processor) {
        try {
            processor.reset();
            taskProcessor.onClose();
            shufflingSenders.values().forEach(ShufflingSender::close);
            shufflingReceivers.values().forEach(ShufflingReceiver::close);
        } finally {
            afterProcessing();
        }
    }

    private void handleResult(boolean result, Throwable e) {
        if (result) {
            return;
        }
        try {
            afterProcessing();
        } finally {
            if (e == null) {
                completeTaskExecution();
            } else {
                handleProcessingError(e);
            }
        }
    }

    private void onStart() {
        Producer[] producers = this.producers.toArray(new Producer[this.producers.size()]);
        Consumer[] consumers = this.consumers.toArray(new Consumer[this.consumers.size()]);

        taskProcessor = taskProcessorFactory.getTaskProcessor(
                producers,
                consumers,
                jobContext,
                processorContext,
                processor,
                vertex,
                taskID
        );

        finalizationStarted = false;
        isFinalizationNotified = false;
        int size = shufflingSenders.values().size();
        sendersArray = shufflingSenders.values().toArray(new ShufflingSender[size]);
    }

    private void afterProcessing() {
        try {
            processor.after(processorContext);
        } catch (Throwable error) {
            handleProcessingError(error);
        }
    }

    private boolean process(BooleanHolder didWorkHolder, TaskProcessor processor) throws Exception {
        if (!sendersFlushed) {
            if (!checkIfSendersFlushed()) {
                return true;
            }
        }

        if (((isFinalizationNotified) && (!finalizationStarted))) {
            didWorkHolder.set(false);
            return true;
        }

        boolean success = processor.process();
        boolean activity = processor.consumed() || processor.produced();
        didWorkHolder.set(activity);

        if (((!activity) && (success))) {
            if (checkProducersClosed()) {
                processor.onProducersWriteFinished();
                return true;
            }

            if (processor.isFinalized()) {
                return handleProcessorFinalized(processor);
            } else {
                return handleProcessorInProgress(processor);
            }
        }

        return true;
    }

    private boolean handleProcessorInProgress(TaskProcessor processor) {
        if (processor.producersReadFinished()) {
            notifyFinalizationStarted();
            if (finalizationStarted) {
                processor.startFinalization();
            }
        }

        return true;
    }

    private boolean handleProcessorFinalized(TaskProcessor processor) {
        if (!checkIfSendersFlushed()) {
            return true;
        }

        if (!sendersClosed) {
            for (ShufflingSender sender : sendersArray) {
                sender.close();
            }
            sendersClosed = true;
            return true;
        }

        if (receiversClosed) {
            return false;
        }

        if (checkReceiversClosed()) {
            processor.onReceiversClosed();
            return true;
        }

        return true;
    }

    private boolean checkProducersClosed() {
        if (producersClosed || activeProducersCounter.get() > 0) {
            return false;
        }
        producersClosed = true;
        return true;

    }

    private boolean checkReceiversClosed() {
        if (receiversClosed || activeReceiversCounter.get() > 0) {
            return false;
        }
        receiversClosed = true;
        return true;

    }

    private void notifyFinalizationStarted() {
        if (isFinalizationNotified) {
            return;
        }
        runner.handleTaskEvent(this, TaskEvent.TASK_READY_FOR_FINALIZATION);
        isFinalizationNotified = true;
    }

    private boolean checkIfSendersFlushed() {
        boolean success = true;

        for (ShufflingSender sender : sendersArray) {
            success &= sender.isFlushed();
        }

        sendersFlushed = success;
        return success;
    }

    private void handleProcessingError(Throwable error) {
        logger.warning(error.getMessage(), error);

        try {
            runner.handleTaskEvent(this, TaskEvent.TASK_EXECUTION_ERROR, error);
        } catch (Throwable e) {
            logger.warning("Exception in the task message=" + e.getMessage(), e);
        } finally {
            completeTaskExecution(error);
        }
    }

    private void completeTaskExecution() {
        completeTaskExecution(null);
    }

    private void completeTaskExecution(Throwable e) {
        try {
            taskProcessor.onClose();
        } finally {
            runner.handleTaskEvent(this, TaskEvent.TASK_EXECUTION_COMPLETED, e);
        }
    }
}
