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


import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.jet.impl.ringbuffer.ShufflingReceiver;
import com.hazelcast.jet.impl.ringbuffer.ShufflingSender;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.task.processors.TaskProcessorFactory;
import com.hazelcast.jet.impl.runtime.task.processors.ShuffledTaskProcessorFactory;
import com.hazelcast.jet.impl.util.BooleanHolder;
import com.hazelcast.jet.runtime.Consumer;
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.jet.runtime.TaskContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents actual vertex execution
 */
@SuppressWarnings("checkstyle:methodcount")
public class VertexTask extends Task {

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final InterruptedException INTERRUPTED_EXCEPTION =
            new InterruptedException("Execution has been interrupted");

    private final ILogger logger;

    private final Vertex vertex;

    private final VertexRunner runner;
    private final AtomicBoolean interrupted = new AtomicBoolean(false);
    private final AtomicInteger activeProducersCounter = new AtomicInteger(0);
    private final AtomicInteger activeReceiversCounter = new AtomicInteger(0);
    private final AtomicInteger finalizedReceiversCounter = new AtomicInteger(0);
    private final Collection<Consumer> consumers = new CopyOnWriteArrayList<>();
    private final Collection<Producer> producers = new CopyOnWriteArrayList<>();
    private final Map<Address, ShufflingReceiver> shufflingReceivers = new ConcurrentHashMap<>();
    private final Map<Address, ShufflingSender> shufflingSenders = new ConcurrentHashMap<>();
    private final TaskContext taskContext;
    private final Processor processor;
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
                      TaskContext taskContext) {
        this.vertex = vertex;
        this.runner = vertexRunner;
        this.taskContext = taskContext;
        this.processor = taskContext.getProcessor();
        logger = taskContext.getJobContext().getNodeEngine().getLogger(getClass());
    }

    /**
     * Interrupts tasks execution
     *
     * @param error - the reason of the interruption
     */
    public void interrupt(Throwable error) {
        if (interrupted.compareAndSet(false, true)) {
            this.error = error;
        }
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

    public void addConsumers(Collection<? extends Consumer> consumers) {
        this.consumers.addAll(consumers);
    }

    public void addConsumer(Consumer consumer) {
        consumers.add(consumer);
    }

    public void addProducer(Producer producer) {
        this.producers.add(producer);
        producer.registerCompletionHandler(p -> activeProducersCounter.decrementAndGet());
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
        return vertex;
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
    @Override
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

    public void start() {
        Producer[] producers = this.producers.toArray(new Producer[this.producers.size()]);
        Consumer[] consumers = this.consumers.toArray(new Consumer[this.consumers.size()]);

        taskProcessor = getTaskProcessorFactory().getTaskProcessor(producers, consumers, taskContext, processor);
        finalizationStarted = false;
        isFinalizationNotified = false;
        int size = shufflingSenders.values().size();
        sendersArray = shufflingSenders.values().toArray(new ShufflingSender[size]);
    }

    /***
     * Will be invoked immediately before task was submitted into the executor,
     * strictly from executor-thread
     */
    public void beforeProcessing() {
        try {
            processor.before(taskContext);
        } catch (Throwable error) {
            afterProcessing();
            handleProcessingError(error);
        }
    }

    public void complete() {
        for (Consumer consumer : consumers) {
            consumer.close();
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

    private void afterProcessing() {
        try {
            processor.after();
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

    private TaskProcessorFactory getTaskProcessorFactory() {
        int clusterSize = taskContext.getJobContext().getNodeEngine().getClusterService().getSize();
        if ((clusterSize > 1 && hasDistributedOutputEdge(vertex)) || (vertex.getSinks().size() > 0)) {
            return new ShuffledTaskProcessorFactory();
        }
        return new TaskProcessorFactory();
    }

    private boolean hasDistributedOutputEdge(Vertex vertex) {
        DAG dag = taskContext.getJobContext().getDAG();
        for (Edge edge : dag.getOutputEdges(vertex)) {
            if (!edge.isLocal()) {
                return true;
            }
        }
        return false;
    }
}
