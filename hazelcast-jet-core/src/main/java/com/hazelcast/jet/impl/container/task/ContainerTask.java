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

package com.hazelcast.jet.impl.container.task;


import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.DataWriter;
import com.hazelcast.jet.executor.TaskContext;
import com.hazelcast.jet.impl.actor.ComposedActor;
import com.hazelcast.jet.impl.actor.ObjectActor;
import com.hazelcast.jet.impl.actor.ObjectConsumer;
import com.hazelcast.jet.impl.actor.ObjectProducer;
import com.hazelcast.jet.impl.actor.RingbufferActor;
import com.hazelcast.jet.impl.actor.shuffling.ShufflingActor;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.impl.container.ContainerContext;
import com.hazelcast.jet.impl.container.DataChannel;
import com.hazelcast.jet.impl.container.DefaultProcessorContext;
import com.hazelcast.jet.impl.container.ProcessingContainer;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.util.BooleanHolder;
import com.hazelcast.jet.processor.Processor;
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
 * Interface which represents container's execution task
 */
@SuppressWarnings("checkstyle:methodcount")
public class ContainerTask extends Task {

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final InterruptedException INTERRUPTED_EXCEPTION =
            new InterruptedException("Execution has been interrupted");

    private final ILogger logger;

    private final int taskID;

    private final Vertex vertex;

    private final NodeEngine nodeEngine;

    private final ProcessingContainer container;
    private final Processor processor;
    private final ContainerContext containerContext;
    private final JobContext jobContext;
    private final TaskProcessorFactory taskProcessorFactory;
    private final AtomicBoolean interrupted = new AtomicBoolean(false);
    private final AtomicInteger activeProducersCounter = new AtomicInteger(0);
    private final AtomicInteger activeReceiversCounter = new AtomicInteger(0);
    private final AtomicInteger finalizedReceiversCounter = new AtomicInteger(0);
    private final Collection<ObjectConsumer> consumers = new CopyOnWriteArrayList<>();
    private final Collection<ObjectProducer> producers = new CopyOnWriteArrayList<>();
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
    private boolean containerFinalizationNotified;
    private volatile boolean finalizationStarted;
    private volatile Throwable error;

    public ContainerTask(ProcessingContainer container,
                         Vertex vertex,
                         TaskProcessorFactory taskProcessorFactory,
                         int taskID,
                         TaskContext taskContext) {
        this.taskID = taskID;
        this.vertex = vertex;
        this.container = container;
        this.taskContext = taskContext;
        this.taskProcessorFactory = taskProcessorFactory;
        containerContext = container.getContainerContext();
        jobContext = container.getJobContext();
        nodeEngine = container.getJobContext().getNodeEngine();
        Supplier<Processor> processorFactory = container.getContainerProcessorFactory();
        processor = processorFactory == null ? null : processorFactory.get();
        processorContext = new DefaultProcessorContext(taskContext, containerContext);
        logger = nodeEngine.getLogger(getClass());
    }

    /**
     * Start tasks' execution
     * Initialize initial state of the task
     *
     * @param producers - list of the input producers
     */
    public void start(List<? extends ObjectProducer> producers) {
        if (producers != null && !producers.isEmpty()) {
            for (ObjectProducer producer : producers) {
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
     * @param channel         - data channel for corresponding edge
     * @param edge            - corresponding edge
     * @param targetContainer - source container of the channel
     * @return - composed actor with actors of channel
     */
    public ComposedActor registerOutputChannel(DataChannel channel, Edge edge, ProcessingContainer targetContainer) {
        List<ObjectActor> actors = new ArrayList<ObjectActor>(targetContainer.getContainerTasks().length);

        for (int i = 0; i < targetContainer.getContainerTasks().length; i++) {
            ObjectActor actor = new RingbufferActor(nodeEngine, jobContext, this, vertex, edge);

            if (channel.isShuffled()) {
                //output
                actor = new ShufflingActor(actor, nodeEngine);
            }
            actors.add(actor);
        }

        ComposedActor composed = new ComposedActor(this, actors, vertex, edge, containerContext);
        consumers.add(composed);

        return composed;
    }

    /**
     * Handled on input producer's completion
     *
     * @param producer - finished input producer
     */
    public void handleProducerCompleted(ObjectProducer producer) {
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
        return containerContext.getVertex();
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
                    container.handleTaskEvent(this, TaskEvent.TASK_EXECUTION_COMPLETED, getError());
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
        ObjectProducer[] producers = this.producers.toArray(new ObjectProducer[this.producers.size()]);
        ObjectConsumer[] consumers = this.consumers.toArray(new ObjectConsumer[this.consumers.size()]);

        taskProcessor = taskProcessorFactory.getTaskProcessor(
                producers,
                consumers,
                containerContext,
                processorContext,
                processor,
                vertex,
                taskID
        );

        finalizationStarted = false;
        containerFinalizationNotified = false;
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

        if (((containerFinalizationNotified) && (!finalizationStarted))) {
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
        if (containerFinalizationNotified) {
            return;
        }
        container.handleTaskEvent(this, TaskEvent.TASK_READY_FOR_FINALIZATION);
        containerFinalizationNotified = true;
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
            container.handleTaskEvent(this, TaskEvent.TASK_EXECUTION_ERROR, error);
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
            container.handleTaskEvent(this, TaskEvent.TASK_EXECUTION_COMPLETED, e);
        }
    }
}
