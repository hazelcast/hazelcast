/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.NIOThread;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class IOReactorImpl implements IOReactor {

    private static final int SHUTDOWN_TIMEOUT_SECONDS = 3;

    private final ILogger logger;
    private final Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<Runnable>();
    private final Selector selector;
    private final IOEventLoop eventLoop;
    private final IOReactorOutOfMemoryHandler oomeHandler;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final ReactorThread reactorThread;
    private volatile boolean isAlive = true;

    public IOReactorImpl(ThreadGroup threadGroup, String threadName, ILogger logger,
                         IOReactorOutOfMemoryHandler oomeHandler, IOEventLoopFactory eventLoopFactory) {
        this.logger = logger;
        this.oomeHandler = oomeHandler;
        try {
            selector = Selector.open();
        } catch (final IOException e) {
            throw new HazelcastException("Failed to open a Selector", e);
        }

        this.reactorThread = new ReactorThread(threadGroup, threadName);
        this.eventLoop = eventLoopFactory.create(this);
    }

    @Override
    public boolean isAlive() {
        return isAlive;
    }

    @Override
    public String getName() {
        return reactorThread.getName();
    }

    @Override
    public IOReactorOutOfMemoryHandler getOutOfMemoryHandler() {
        return oomeHandler;
    }

    @Override
    public ILogger getLogger() {
        return logger;
    }

    @Override
    public Selector getSelector() {
        return selector;
    }

    @Override
    public void dumpPerformanceMetrics(StringBuffer sb) {
        eventLoop.dumpPerformanceMetrics(sb);
    }

    @Override
    public void start() {
        reactorThread.start();
    }

    @Override
    public void shutdown() {
        taskQueue.clear();
        try {
            addTask(new Runnable() {
                @Override
                public void run() {
                    isAlive = false;
                    shutdownLatch.countDown();
                }
            });
            reactorThread.interrupt();
        } catch (Throwable t) {
            Logger.getLogger(IOReactorImpl.class).finest("Exception while waiting for shutdown", t);
        }
    }

    @Override
    public void awaitShutdown() {
        try {
            shutdownLatch.await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException t) {
            Logger.getLogger(IOReactorImpl.class).finest("Exception while waiting for shutdown", t);
        }
    }

    @Override
    public void addTask(Runnable runnable) {
        taskQueue.add(runnable);
    }

    @Override
    public void processTasks() {
        //noinspection WhileLoopSpinsOnField
        while (isAlive) {
            final Runnable runnable = taskQueue.poll();
            if (runnable == null) {
                return;
            }
            runnable.run();
        }
    }

    @Override
    public void wakeup() {
        selector.wakeup();
    }

    private final class ReactorThread extends Thread implements NIOThread {

        private ReactorThread(ThreadGroup threadGroup, String threadName) {
            super(threadGroup, threadName);
        }

        @Override
        public void run() {
            try {
                eventLoop.run();
            } catch (OutOfMemoryError e) {
                oomeHandler.handle(e);
            } catch (Throwable e) {
                logger.warning("Unhandled exception in " + getName(), e);
            } finally {
                isAlive = false;
                closeSelectorQuietly();
            }
        }

        private void closeSelectorQuietly() {
            try {
                if (logger.isFinestEnabled()) {
                    logger.finest("Closing selector " + getName());
                }
                selector.close();
            } catch (Throwable e) {
                //todo: Why are we not using the logger here?
                Logger.getLogger(IOReactorImpl.class).finest("Exception while closing selector", e);
            }
        }
    }
}
