/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.Thread.currentThread;

public abstract class NioPipeline implements MigratablePipeline, Closeable, Runnable {

    protected static final int LOAD_BALANCING_HANDLE = 0;
    protected static final int LOAD_BALANCING_BYTE = 1;
    protected static final int LOAD_BALANCING_FRAME = 2;

    // for the time being we configure using a int until we have decided which load strategy to use.
    protected final int loadType = Integer.getInteger("hazelcast.io.load", LOAD_BALANCING_BYTE);

    @Probe
    final SwCounter processCount = newSwCounter();
    final ILogger logger;
    final Channel channel;

    volatile NioThread owner;
    private SelectionKey selectionKey;
    private final SocketChannel socketChannel;
    private final int initialOps;
    private final IOBalancer ioBalancer;
    private final ChannelErrorHandler errorHandler;
    private final AtomicReference<TaskNode> delayedTaskStack = new AtomicReference<TaskNode>();
    @Probe
    private volatile int ownerId;
    // counts the number of migrations that have happened so far
    @Probe
    private final SwCounter startedMigrations = newSwCounter();
    @Probe
    private final SwCounter completedMigrations = newSwCounter();

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

    @Probe(level = DEBUG)
    private long opsInterested() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.interestOps();
    }

    @Probe(level = DEBUG)
    private long opsReady() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.readyOps();
    }

    @Override
    public NioThread owner() {
        return owner;
    }

    void start() {
        addTaskAndWakeup(new NioPipelineTask(this) {
            @Override
            protected void run0() {
                try {
                    getSelectionKey();
                } catch (Throwable t) {
                    onError(t);
                }
            }
        });
    }

    private SelectionKey getSelectionKey() throws IOException {
        if (selectionKey == null) {
            selectionKey = socketChannel.register(owner.getSelector(), initialOps, this);
        }
        return selectionKey;
    }

    final void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    final void registerOp(int operation) throws IOException {
        SelectionKey selectionKey = getSelectionKey();
        selectionKey.interestOps(selectionKey.interestOps() | operation);
    }

    final void unregisterOp(int operation) throws IOException {
        SelectionKey selectionKey = getSelectionKey();
        int interestOps = selectionKey.interestOps();
        if ((interestOps & operation) != 0) {
            selectionKey.interestOps(interestOps & ~operation);
        }
    }

    abstract void publishMetrics();

    /**
     * Called when there are bytes available for reading, or space available to
     * write.
     *
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
     * Forces this NioPipeline to wakeup by scheduling this on the owner using
     * {@link NioThread#addTaskAndWakeup(Runnable)}
     *
     * This method can be called by any thread. It is a pretty expensive method
     * because it will cause the {@link Selector#wakeup()} method to be called.
     *
     * This call can safely be made during the migration of the pipeline.
     */
    final void wakeup() {
        addTaskAndWakeup(this);
    }

    /**
     * Adds a task to be executed on the {@link NioThread owner}.
     *
     * This task is scheduled on the task queue of the owning {@link NioThread}.
     *
     * If the pipeline is currently migrating, this method will make sure the
     * task ends up at the new owner.
     *
     * It is extremely important that this task takes very little time because
     * otherwise it could cause a lot of problems in the IOSystem.
     *
     * This method can be called by any thread. It is a pretty expensive method
     * because it will cause the {@link Selector#wakeup()} method to be called.
     *
     * @param task the task to add.
     */
    final void addTaskAndWakeup(Runnable task) {
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
            addTaskAndWakeup(this);
        }
    }

    /**
     * Is called when the {@link #process()} throws an exception.
     *
     * This method should only be called by the current {@link NioThread owner}.
     *
     * The idiom to use a handler is:
     * <code>
     * try{
     *     handler.handle();
     * } catch(Throwable t) {
     *    handler.onError(t);
     * }
     * </code>
     *
     * @param cause
     */
    public void onError(Throwable cause) {
        if (cause instanceof InterruptedException) {
            currentThread().interrupt();
        }

        if (selectionKey != null) {
            selectionKey.cancel();
        }

        errorHandler.onError(channel, cause);
    }

    /**
     * Migrates this pipeline to a different owner.
     * The migration logic is rather simple:
     * <p><ul>
     * <li>Submit a de-registration task to a current NioThread</li>
     * <li>The de-registration task submits a registration task to the new NioThread</li>
     * </ul></p>
     *
     * @param newOwner target NioThread this handler migrates to
     */
    @Override
    public final void requestMigration(NioThread newOwner) {
        // todo: what happens when owner null.
        owner.addTaskAndWakeup(new StartMigrationTask(newOwner));
    }

    private class StartMigrationTask implements Runnable {
        private final NioThread newOwner;

        StartMigrationTask(NioThread newOwner) {
            this.newOwner = newOwner;
        }

        @Override
        public void run() {
            // if there is no change, we are done
            if (owner == newOwner) {
                return;
            }

            publishMetrics();

            try {
                startMigration(newOwner);
            } catch (Throwable t) {
                onError(t);
            }
        }

        // This method run on the owning NioThread
        private void startMigration(final NioThread newOwner) throws IOException {
            assert owner == currentThread() : "startMigration can only run on the owning NioThread";
            assert owner != newOwner : "newOwner can't be the same as the existing owner";

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

                selectionKey = getSelectionKey();
                registerOp(initialOps);
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
