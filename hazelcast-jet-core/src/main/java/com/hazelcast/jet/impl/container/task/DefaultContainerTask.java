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


import com.hazelcast.jet.api.actor.ComposedActor;
import com.hazelcast.jet.api.actor.ObjectActor;
import com.hazelcast.jet.api.actor.ObjectConsumer;
import com.hazelcast.jet.api.actor.ObjectProducer;
import com.hazelcast.jet.api.actor.ProducerCompletionHandler;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.container.DataChannel;
import com.hazelcast.jet.api.container.ProcessingContainer;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.container.task.TaskEvent;
import com.hazelcast.jet.api.container.task.TaskProcessor;
import com.hazelcast.jet.api.container.task.TaskProcessorFactory;
import com.hazelcast.jet.api.executor.Payload;
import com.hazelcast.jet.api.processor.ContainerProcessorFactory;
import com.hazelcast.jet.impl.actor.DefaultComposedActor;
import com.hazelcast.jet.impl.actor.RingBufferActor;
import com.hazelcast.jet.impl.actor.shuffling.ShufflingActor;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.impl.container.DefaultProcessorContext;
import com.hazelcast.jet.spi.dag.Edge;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.data.DataWriter;
import com.hazelcast.jet.spi.executor.TaskContext;
import com.hazelcast.jet.spi.processor.ContainerProcessor;
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

public class DefaultContainerTask extends AbstractTask
        implements ContainerTask {
    private final ILogger logger;

    private final int taskID;

    private final Vertex vertex;

    private final NodeEngine nodeEngine;

    private final ProcessingContainer container;
    private final ContainerProcessor processor;
    private final ContainerContext containerContext;
    private final ApplicationContext applicationContext;
    private final TaskProcessorFactory taskProcessorFactory;
    private final AtomicBoolean interrupted = new AtomicBoolean(false);
    private final AtomicInteger activeProducersCounter = new AtomicInteger(0);
    private final AtomicInteger activeReceiversCounter = new AtomicInteger(0);
    private final AtomicInteger finalizedReceiversCounter = new AtomicInteger(0);
    private final Collection<ObjectConsumer> consumers = new CopyOnWriteArrayList<ObjectConsumer>();
    private final Collection<ObjectProducer> producers = new CopyOnWriteArrayList<ObjectProducer>();
    private final Map<Address, ShufflingReceiver> shufflingReceivers = new ConcurrentHashMap<Address, ShufflingReceiver>();
    private final Map<Address, ShufflingSender> shufflingSenders = new ConcurrentHashMap<Address, ShufflingSender>();
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

    public DefaultContainerTask(ProcessingContainer container,
                                Vertex vertex,
                                TaskProcessorFactory taskProcessorFactory,
                                int taskID,
                                TaskContext taskContext) {
        this.taskID = taskID;
        this.vertex = vertex;
        this.container = container;
        this.taskContext = taskContext;
        this.taskProcessorFactory = taskProcessorFactory;
        this.containerContext = container.getContainerContext();
        this.applicationContext = container.getApplicationContext();
        this.nodeEngine = container.getApplicationContext().getNodeEngine();
        this.logger = this.nodeEngine.getLogger(DefaultContainerTask.class);
        ContainerProcessorFactory processorFactory = container.getContainerProcessorFactory();
        this.processor = processorFactory == null ? null : processorFactory.getProcessor(vertex);
        this.processorContext = new DefaultProcessorContext(taskContext, this.containerContext);
    }

    @Override
    public void start(List<? extends ObjectProducer> producers) {
        if ((producers != null) && (producers.size() > 0)) {
            for (ObjectProducer producer : producers) {
                this.producers.add(producer);

                producer.registerCompletionHandler(new ProducerCompletionHandler() {
                    @Override
                    public void onComplete(ObjectProducer producer) {
                        handleProducerCompleted(producer);
                    }
                });
            }
        }

        onStart();
    }

    @Override
    public void interrupt(Throwable error) {
        if (this.interrupted.compareAndSet(false, true)) {
            this.error = error;
        }
    }

    @Override
    public void registerSinkWriters(List<DataWriter> sinkWriters) {
        this.consumers.addAll(sinkWriters);
    }

    @Override
    public ComposedActor registerOutputChannel(DataChannel channel, Edge edge, ProcessingContainer targetContainer) {
        List<ObjectActor> actors = new ArrayList<ObjectActor>(targetContainer.getContainerTasks().length);

        for (int i = 0; i < targetContainer.getContainerTasks().length; i++) {
            ObjectActor actor = new RingBufferActor(this.nodeEngine, this.applicationContext, this, this.vertex, edge);

            if (channel.isShuffled()) {
                //output
                actor = new ShufflingActor(actor, this.nodeEngine, this.containerContext);
            }

            actors.add(actor);
        }

        ComposedActor composed = new DefaultComposedActor(this, actors, this.vertex, edge, this.containerContext);
        this.consumers.add(composed);

        return composed;
    }

    @Override
    public void handleProducerCompleted(ObjectProducer actor) {
        this.activeProducersCounter.decrementAndGet();
    }

    @Override
    public void registerShufflingReceiver(Address address, ShufflingReceiver receiver) {
        this.shufflingReceivers.put(address, receiver);

        receiver.registerCompletionHandler(new ProducerCompletionHandler() {
            @Override
            public void onComplete(ObjectProducer producer) {
                activeReceiversCounter.decrementAndGet();
            }
        });
    }

    @Override
    public TaskContext getTaskContext() {
        return this.taskContext;
    }

    @Override
    public ShufflingReceiver getShufflingReceiver(Address endPoint) {
        return this.shufflingReceivers.get(endPoint);
    }

    @Override
    public Vertex getVertex() {
        return this.containerContext.getVertex();
    }

    @Override
    public void startFinalization() {
        this.finalizationStarted = true;
    }

    @Override
    public void registerShufflingSender(Address address, ShufflingSender sender) {
        this.shufflingSenders.put(address, sender);
    }

    @Override
    public void init() {
        this.error = null;
        this.taskProcessor.onOpen();
        this.interrupted.set(false);
        this.sendersClosed = false;
        this.receiversClosed = false;
        this.sendersFlushed = false;
        this.producersClosed = false;
        this.activeProducersCounter.set(this.producers.size());
        this.activeReceiversCounter.set(this.shufflingReceivers.values().size());
        this.finalizedReceiversCounter.set(this.shufflingReceivers.values().size());
    }

    @Override
    public void beforeProcessing() {
        try {
            this.processor.beforeProcessing(this.processorContext);
        } catch (Throwable error) {
            afterProcessing();
            handleProcessingError(error);
        }
    }

    @Override
    public boolean executeTask(Payload payload) {
        TaskProcessor processor = this.taskProcessor;
        boolean classLoaderChanged = false;
        ClassLoader classLoader = null;

        if (this.contextClassLoader != null) {
            classLoader = Thread.currentThread().getContextClassLoader();
            if (this.contextClassLoader != classLoader) {
                Thread.currentThread().setContextClassLoader(this.contextClassLoader);
                classLoaderChanged = true;
            }
        }

        try {
            if (this.interrupted.get()) {
                try {
                    onInterrupt(processor);
                } finally {
                    this.container.handleTaskEvent(this, TaskEvent.TASK_INTERRUPTED, this.error);
                }

                return false;
            }

            boolean result;
            Throwable error = null;

            try {
                result = process(payload, processor);
            } catch (Throwable e) {
                result = false;
                payload.set(false);
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

    private void onInterrupt(TaskProcessor processor) {
        try {
            processor.reset();
            this.taskProcessor.onClose();
            for (ShufflingSender shufflingSender : this.shufflingSenders.values()) {
                shufflingSender.close();
            }

            for (ShufflingReceiver shufflingReceiver : this.shufflingReceivers.values()) {
                shufflingReceiver.close();
            }
        } finally {
            afterProcessing();
        }
    }

    private void handleResult(boolean result, Throwable e) {
        if (!result) {
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
    }

    private void onStart() {
        ObjectProducer[] producers = this.producers.toArray(new ObjectProducer[this.producers.size()]);
        ObjectConsumer[] consumers = this.consumers.toArray(new ObjectConsumer[this.consumers.size()]);

        this.taskProcessor = this.taskProcessorFactory.getTaskProcessor(
                producers,
                consumers,
                this.containerContext,
                this.processorContext,
                this.processor,
                this.vertex,
                this.taskID
        );

        this.finalizationStarted = false;
        this.containerFinalizationNotified = false;
        int size = this.shufflingSenders.values().size();
        this.sendersArray =
                this.shufflingSenders.values().toArray(new ShufflingSender[size]);
    }

    private void afterProcessing() {
        try {
            this.processor.afterProcessing(this.processorContext);
        } catch (Throwable error) {
            handleProcessingError(error);
        }
    }

    private boolean process(Payload payload, TaskProcessor processor) throws Exception {
        if (!this.sendersFlushed) {
            if (!checkIfSendersFlushed()) {
                return true;
            }
        }

        if (((this.containerFinalizationNotified) && (!this.finalizationStarted))) {
            payload.set(false);
            return true;
        }

        boolean success = processor.process();
        boolean activity = processor.consumed() || processor.produced();
        payload.set(activity);

        if (((!activity) && (success))) {
            checkActiveProducers(processor);

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
            if (this.finalizationStarted) {
                processor.startFinalization();
            }
        }

        return true;
    }

    private boolean handleProcessorFinalized(TaskProcessor processor) {
        if (!checkIfSendersFlushed()) {
            return true;
        }

        if (!this.sendersClosed) {
            for (ShufflingSender sender : this.sendersArray) {
                sender.close();
            }

            this.sendersClosed = true;
            return true;
        }

        if (this.receiversClosed) {
            return false;
        }

        if (checkReceiversClosed()) {
            processor.onReceiversClosed();
            return true;
        }

        return true;
    }

    private boolean checkProducersClosed() {
        if ((!this.producersClosed) && (this.activeProducersCounter.get() <= 0)) {
            this.producersClosed = true;
            return true;
        }

        return false;
    }

    private boolean checkReceiversClosed() {
        if (((!this.receiversClosed) && (this.activeReceiversCounter.get() <= 0))) {
            this.receiversClosed = true;
            return true;
        }

        return false;
    }

    private void checkActiveProducers(TaskProcessor processor) {
        if ((!processor.hasActiveProducers())) {
            this.activeProducersCounter.set(0);
            this.finalizedReceiversCounter.set(0);
            this.activeReceiversCounter.set(0);
        }
    }

    private void notifyFinalizationStarted() {
        if (!this.containerFinalizationNotified) {
            this.container.handleTaskEvent(this, TaskEvent.TASK_READY_FOR_FINALIZATION);
            this.containerFinalizationNotified = true;
        }
    }

    private boolean checkIfSendersFlushed() {
        boolean success = true;

        for (ShufflingSender sender : this.sendersArray) {
            success &= sender.isFlushed();
        }

        this.sendersFlushed = success;
        return success;
    }

    private void handleProcessingError(Throwable error) {
        this.logger.warning("Exception in the task message=" + error.getMessage(), error);

        try {
            this.container.handleTaskEvent(this, TaskEvent.TASK_EXECUTION_ERROR, error);
        } catch (Throwable e) {
            this.logger.warning("Exception in the task message=" + e.getMessage(), e);
        }
    }

    private void completeTaskExecution() {
        try {
            System.out.println("completeTaskExecution=" + applicationContext.getName());
            this.taskProcessor.onClose();
        } finally {
            this.container.handleTaskEvent(this, TaskEvent.TASK_EXECUTION_COMPLETED);
        }
    }
}
