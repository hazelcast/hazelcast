/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelHandler;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_PIPELINE_COMPLETED_MIGRATIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_PIPELINE_OPS_INTERESTED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_PIPELINE_OPS_READY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_PIPELINE_OWNER_ID;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_PIPELINE_PROCESS_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_PIPELINE_STARTED_MIGRATIONS;
import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.Thread.currentThread;

public abstract class NioPipeline implements MigratablePipeline, Runnable {

    protected static final int LOAD_BALANCING_HANDLE = 0;
    protected static final int LOAD_BALANCING_BYTE = 1;
    protected static final int LOAD_BALANCING_FRAME = 2;

    // for the time being we configure using an int until we have decided which load strategy to use.
    protected final int loadType = Integer.getInteger("hazelcast.io.load", LOAD_BALANCING_BYTE);

    // the number of time the NioPipeline.process() method has been called.
    @Probe(name = NETWORKING_METRIC_NIO_PIPELINE_PROCESS_COUNT, level = DEBUG)
    protected final SwCounter processCount = newSwCounter();
    protected final ILogger logger;
    protected final NioChannel channel;
    protected final SocketChannel socketChannel;
    // needs to be volatile because it can be accessed concurrently (only the owner will modify).
    // Owner can be null if the pipeline is being migrated.
    protected volatile NioThread owner;

    // in case of outbound pipeline, this selectionKey is only changed when the pipeline is scheduled
    // (so a single thread has claimed possession of the pipeline)
    protected volatile SelectionKey selectionKey;
    private final ChannelErrorHandler errorHandler;
    private final int initialOps;
    private final IOBalancer ioBalancer;
    private final AtomicReference<TaskNode> delayedTaskStack = new AtomicReference<>();
    @Probe(name = NETWORKING_METRIC_NIO_PIPELINE_OWNER_ID, level = DEBUG)
    private volatile int ownerId;
    // counts the number of migrations that have happened so far
    @Probe(name = NETWORKING_METRIC_NIO_PIPELINE_STARTED_MIGRATIONS, level = DEBUG)
    private final SwCounter startedMigrations = newSwCounter();
    @Probe(name = NETWORKING_METRIC_NIO_PIPELINE_COMPLETED_MIGRATIONS, level = DEBUG)
    private final SwCounter completedMigrations = newSwCounter();
    private volatile NioThread newOwner;

    NioPipeline(NioChannel channel,
                NioThread owner,
                ChannelErrorHandler errorHandler,
                int initialOps,
                ILogger logger,
                IOBalancer ioBalancer) {
        this.channel = channel;
        this.socketChannel = channel.socketChannel();
        this.owner = owner;
        this.ownerId = owner.id;
        this.logger = logger;
        this.initialOps = initialOps;
        this.ioBalancer = ioBalancer;
        this.errorHandler = errorHandler;
    }

    public Channel getChannel() {
        return channel;
    }

    @Probe(name = NETWORKING_METRIC_NIO_PIPELINE_OPS_INTERESTED, level = DEBUG)
    private long opsInterested() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.interestOps();
    }

    @Probe(name = NETWORKING_METRIC_NIO_PIPELINE_OPS_READY, level = DEBUG)
    private long opsReady() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.readyOps();
    }

    /**
     * Returns the {@link NioThread} owning this pipeline.
     * It can be null when the pipeline is being migrated between threads.
     * <p>
     * Owner is the thread executing the pipeline.
     *
     * @return thread owning the pipeline or <code>null</code> when the pipeline is being migrated
     */
    @Override
    public NioThread owner() {
        return owner;
    }

    void start() {
        owner.addTaskAndWakeup(() -> {
            try {
                initSelectionKey();
                process();
            } catch (Throwable t) {
                onError(t);
            }
        });
    }

    final void initSelectionKey() throws ClosedChannelException {
        initSelectionKey(owner.getSelector(), initialOps);
    }

    final void initSelectionKey(Selector selector, int ops) throws ClosedChannelException {
        selectionKey = socketChannel.register(selector, ops, NioPipeline.this);
    }

    final void registerOp(int operation) {
        selectionKey.interestOps(selectionKey.interestOps() | operation);
    }

    final void unregisterOp(int operation) {
        int interestOps = selectionKey.interestOps();
        if ((interestOps & operation) != 0) {
            selectionKey.interestOps(interestOps & ~operation);
        }
    }

    abstract void publishMetrics();

    /**
     * Called when the pipeline needs to be processed.
     * <p>
     * Any exception that leads to a termination of the connection like an
     * IOException should not be dealt with in the handle method but should
     * be propagated. The reason behind this is that the handle logic already
     * is complicated enough and by pulling it out the flow will be easier to
     * understand.
     *
     * @throws Exception
     */
    abstract void process() throws Exception;

    /**
     * Adds a task to be executed on the {@link NioThread owner}.
     * <p>
     * This task is scheduled on the task queue of the owning {@link NioThread}.
     * <p>
     * If the pipeline is currently migrating, this method will make sure the
     * task ends up at the new owner.
     * <p>
     * It is extremely important that this task takes very little time because
     * otherwise it could cause a lot of problems in the IOSystem.
     * <p>
     * This method can be called by any thread. It is a pretty expensive method
     * because it will cause the {@link Selector#wakeup()} method to be called.
     *
     * @param task the task to add.
     */
    final void ownerAddTaskAndWakeup(Runnable task) {
        // in this loop we are going to either send the task to the owner
        // or store the delayed task to be picked up as soon as the migration
        // completes
        for (; ; ) {
            NioThread localOwner = owner;
            if (localOwner != null) {
                // there is an owner, lets send the task.
                localOwner.addTaskAndWakeup(task);
                return;
            }

            // there is no owner, so we put the task on the delayedTaskStack
            TaskNode old = delayedTaskStack.get();
            TaskNode update = new TaskNode(task, old);
            if (delayedTaskStack.compareAndSet(old, update)) {
                break;
            }
        }

        NioThread localOwner = owner;
        if (localOwner != null) {
            // an owner was set, but we have no guarantee that he has seen our task.
            // So lets try to reschedule the delayed tasks to make sure they get executed.
            restoreTasks(localOwner, delayedTaskStack.getAndSet(null), true);
        }
    }

    private void restoreTasks(NioThread owner, TaskNode node, boolean wakeup) {
        if (node == null) {
            return;
        }

        // we restore in the opposite order so that we get fifo.
        restoreTasks(owner, node.next, false);
        if (wakeup) {
            owner.addTaskAndWakeup(node.task);
        } else {
            owner.addTask(node.task);
        }
    }

    @Override
    public final void run() {
        if (owner == currentThread()) {
            try {
                process();
            } catch (Throwable t) {
                onError(t);
            }
        } else {
            // the pipeline is executed on the wrong IOThread, so send the
            // pipeline to the right IO Thread to be executed.
            ownerAddTaskAndWakeup(this);
        }
    }

    /**
     * Is called when the {@link #process()} throws a {@link Throwable}.
     * <p>
     * This method should only be called by the current {@link NioThread owner}.
     * <p>
     * The idiom to use a pipeline is:
     * <code>
     * try{
     * pipeline.process();
     * } catch(Throwable t) {
     * pipeline.onError(t);
     * }
     * </code>
     *
     * @param error
     */
    public void onError(Throwable error) {
        if (error instanceof InterruptedException) {
            currentThread().interrupt();
        }

        SelectionKey selectionKey = this.selectionKey;
        if (selectionKey != null) {
            selectionKey.cancel();
        }

        // mechanism for the handlers to intercept and modify the
        // throwable.
        try {
            for (ChannelHandler handler : handlers()) {
                handler.interceptError(error);
            }
        } catch (Throwable newError) {
            error = newError;
        }

        errorHandler.onError(channel, error);
    }

    /**
     * Returns an Iterable that can iterate over each {@link ChannelHandler} of
     * the pipeline.
     * <p>
     * This method is called only by the {@link #onError(Throwable)}.
     *
     * @return the Iterable.
     */
    protected abstract Iterable<? extends ChannelHandler> handlers();

    /**
     * Migrates this pipeline to a different owner.
     * The migration logic is rather simple:
     * <p><ul>
     * <li>Submit a de-registration task to a current NioThread</li>
     * <li>The de-registration task submits a registration task to the new NioThread</li>
     * </ul>
     *
     * @param newOwner target NioThread this handler migrates to
     */
    @Override
    public final void requestMigration(NioThread newOwner) {
        this.newOwner = newOwner;

        // we can't call wakeup directly unfortunately because wakeup isn't defined on this
        // abstract class and can't be defined due to incompatible return types of the wakeup
        // on the inbound and outbound pipeline.
        if (this instanceof NioOutboundPipeline) {
            ((NioOutboundPipeline) this).wakeup();
        } else {
            ((NioInboundPipeline) this).wakeup();
        }
    }

    boolean migrationRequested() {
        return newOwner != null;
    }

    /**
     * Starts the migration.
     * <p>
     * This method needs to run on a thread that is executing the {@link #process()}  method.
     *
     */
    void startMigration() {
        assert newOwner != null : "newOwner can't be null";
        assert owner != newOwner : "newOwner can't be the same as the existing owner";
        publishMetrics();

        if (!socketChannel.isOpen()) {
            // if the channel is closed, we are done.
            return;
        }

        startedMigrations.inc();

        unregisterOp(initialOps);
        selectionKey.cancel();
        selectionKey = null;
        owner = null;
        ownerId = -1;

        newOwner.addTaskAndWakeup(new CompleteMigrationTask(newOwner));
    }

    private class CompleteMigrationTask implements Runnable {
        private final NioThread newOwner;

        CompleteMigrationTask(NioThread newOwner) {
            this.newOwner = newOwner;
        }

        @Override
        public void run() {
            try {
                assert owner == null;
                owner = newOwner;
                ownerId = newOwner.id;

                // we don't need to wakeup since the io thread will see the delayed tasks.
                restoreTasks(owner, delayedTaskStack.getAndSet(null), false);

                completedMigrations.inc();
                ioBalancer.signalMigrationComplete();

                if (!socketChannel.isOpen()) {
                    return;
                }

                initSelectionKey();

                // and now we set the newOwner to null since we are finished with the migration
                NioPipeline.this.newOwner = null;
            } catch (Throwable t) {
                onError(t);
            }
        }
    }

    private static class TaskNode {
        private final Runnable task;
        private final TaskNode next;

        TaskNode(Runnable task, TaskNode next) {
            this.task = task;
            this.next = next;
        }
    }
}
