/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class AbstractIOSelector extends Thread implements IOSelector {

    private static final int SELECT_WAIT_TIME_MILLIS = 5000;
    private static final int SELECT_FAILURE_PAUSE_MILLIS = 1000;

    private final ILogger logger;

    private final Queue<Runnable> selectorQueue = new ConcurrentLinkedQueue<Runnable>();

    private final int waitTime;

    private final Selector selector;

    private final IOSelectorOutOfMemoryHandler oomeHandler;

    // field doesn't need to be volatile, is only accessed by the IOSelector-thread.
    private boolean running = true;

    public AbstractIOSelector(ThreadGroup threadGroup, String threadName, ILogger logger,
                              IOSelectorOutOfMemoryHandler oomeHandler) {
        super(threadGroup, threadName);
        this.logger = logger;
        this.oomeHandler = oomeHandler;
        // WARNING: This value has significant effect on idle CPU usage!
        this.waitTime = SELECT_WAIT_TIME_MILLIS;
        try {
            selector = Selector.open();
        } catch (final IOException e) {
            throw new HazelcastException("Failed to open a Selector", e);
        }
    }

    @Override
    public final void shutdown() {
        selectorQueue.clear();
        try {
            addTask(new Runnable() {
                @Override
                public void run() {
                    running = false;
                }
            });
            interrupt();
        } catch (Throwable t) {
            logger.finest("Exception while waiting for shutdown", t);
        }
    }

    @Override
    public final void addTask(Runnable task) {
        selectorQueue.add(task);
    }

    @Override
    public final void addTaskAndWakeup(Runnable task) {
        selectorQueue.add(task);
        selector.wakeup();
    }

    private void processSelectionQueue() {
        //noinspection WhileLoopSpinsOnField
        while (running) {
            final Runnable task = selectorQueue.poll();
            if (task == null) {
                return;
            }
            executeTask(task);
        }
    }

    private void executeTask(Runnable task) {
        IOSelector target = getTargetIOSelector(task);
        if (target == this) {
            task.run();
        } else {
            target.addTask(task);
        }
    }

    private IOSelector getTargetIOSelector(Runnable task) {
        if (task instanceof MigratableHandler) {
            return ((MigratableHandler) task).getOwner();
        } else {
            return this;
        }
    }

    @Override
    public final void run() {
        try {
            //noinspection WhileLoopSpinsOnField
            while (running) {
                processSelectionQueue();
                if (!running || isInterrupted()) {
                    if (logger.isFinestEnabled()) {
                        logger.finest(getName() + " is interrupted!");
                    }
                    running = false;
                    return;
                }

                try {
                    int selectedKeyCount = selector.select(waitTime);
                    if (selectedKeyCount == 0) {
                        continue;
                    }
                } catch (Throwable e) {
                    handleSelectFailure(e);
                    continue;
                }
                handleSelectionKeys();
            }
        } catch (OutOfMemoryError e) {
            oomeHandler.handle(e);
        } catch (Throwable e) {
            logger.warning("Unhandled exception in " + getName(), e);
        } finally {
            closeSelector();
        }
    }

    private void closeSelector() {
        if (logger.isFinestEnabled()) {
            logger.finest("Closing selector " + getName());
        }

        try {
            selector.close();
        } catch (Exception e) {
            logger.finest("Exception while closing selector", e);
        }
    }

    protected abstract void handleSelectionKey(SelectionKey sk);

    private void handleSelectionKeys() {
        final Set<SelectionKey> setSelectedKeys = selector.selectedKeys();
        final Iterator<SelectionKey> it = setSelectedKeys.iterator();
        while (it.hasNext()) {
            final SelectionKey sk = it.next();
            it.remove();
            try {
                handleSelectionKey(sk);
            } catch (Throwable e) {
                handleSelectionKeyFailure(e);
            }
        }
    }

    public void handleSelectionKeyFailure(Throwable e) {
        logger.warning("Selector exception at  " + getName() + ", cause= " + e.toString(), e);
        if (e instanceof OutOfMemoryError) {
            oomeHandler.handle((OutOfMemoryError) e);
        }
    }

    @Override
    public final Selector getSelector() {
        return selector;
    }

    private void handleSelectFailure(Throwable e) {
        logger.warning(e.toString(), e);

        // If we don't wait, it can be that a subsequent call will run into an IOException immediately. This can lead to a very
        // hot loop and we don't want that. The same approach is used in Netty.
        try {
            Thread.sleep(SELECT_FAILURE_PAUSE_MILLIS);
        } catch (InterruptedException i) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return getName();
    }
}
