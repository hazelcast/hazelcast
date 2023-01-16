/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.function.Supplier;

abstract class AbstractFJPSupplier implements Supplier<ForkJoinPool> {

    private static final int MAX_PARALLELISM = 256;
    private static final String PROP_PARALLELISM
            = "hazelcast.internal.default.async.executor.parallelism";
    private static final String PROP_MAX_POOL_SIZE
            = "hazelcast.internal.default.async.executor.maxPoolSize";
    private static final String PROP_MIN_RUNNABLE
            = "hazelcast.internal.default.async.executor.minRunnable";

    private final int parallelism;
    private final int maxPoolSize;
    private final int minRunnable;

    @SuppressWarnings("checkstyle:multiplevariabledeclarations")
    AbstractFJPSupplier() {
        String parallelismFromSystemProp = System.getProperty(PROP_PARALLELISM);
        String maxPoolSizeFromSystemProp = System.getProperty(PROP_MAX_POOL_SIZE);
        String minRunnableFromSystemProp = System.getProperty(PROP_MIN_RUNNABLE);

        int parallelism, maxPoolSize, minRunnable;
        if (parallelismFromSystemProp != null) {
            parallelism = Integer.parseInt(parallelismFromSystemProp);
        } else {
            parallelism = Runtime.getRuntime().availableProcessors();
        }

        if (maxPoolSizeFromSystemProp != null) {
            maxPoolSize = Integer.parseInt(maxPoolSizeFromSystemProp);
            parallelism = Integer.min(parallelism, maxPoolSize);
        } else {
            maxPoolSize = Integer.max(parallelism, MAX_PARALLELISM);
        }

        if (minRunnableFromSystemProp != null) {
            minRunnable = Integer.parseInt(minRunnableFromSystemProp);
        } else {
            minRunnable = Integer.max(parallelism / 2, 1);
        }

        this.parallelism = parallelism;
        this.maxPoolSize = maxPoolSize;
        this.minRunnable = minRunnable;
    }

    abstract ForkJoinPool newJDKSpecificFJP(int parallelism, int maxPoolSize, int minRunnable,
                                            ForkJoinPool.ForkJoinWorkerThreadFactory factory,
                                            Thread.UncaughtExceptionHandler handler,
                                            boolean asyncMode);

    @Override
    public ForkJoinPool get() {
        return newForkJoinPool();
    }

    private ForkJoinPool newForkJoinPool() {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = newThreadFactory();
        PrivilegedAction<ForkJoinPool> privilegedAction = () -> {
            Thread.UncaughtExceptionHandler handler = newUncaughtExceptionHandler();
            return newJDKSpecificFJP(parallelism, maxPoolSize, minRunnable,
                    factory, handler, true);
        };
        return AccessController.doPrivileged(privilegedAction);
    }

    private static ForkJoinPool.ForkJoinWorkerThreadFactory newThreadFactory() {
        return pool -> {
            PrivilegedAction<ForkJoinWorkerThread> privilegedAction
                    = () -> new HzForkJoinWorkerThread(pool);
            return AccessController.doPrivileged(privilegedAction);
        };
    }

    private static Thread.UncaughtExceptionHandler newUncaughtExceptionHandler() {
        return new Thread.UncaughtExceptionHandler() {
            ILogger logger = Logger.getLogger(ConcurrencyUtil.class);

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.severe(t + " had an unexpected exception", e);
            }
        };
    }

    private static class HzForkJoinWorkerThread extends ForkJoinWorkerThread {
        protected HzForkJoinWorkerThread(ForkJoinPool pool) {
            super(pool);
            super.setDaemon(true);
            this.setContextClassLoader(ForkJoinPool.class.getClassLoader());
            this.setName("hazelcast.internal.fork.join.worker.thread");
        }
    }
}
