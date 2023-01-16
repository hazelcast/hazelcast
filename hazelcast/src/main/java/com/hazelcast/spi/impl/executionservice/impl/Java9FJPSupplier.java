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

import com.hazelcast.internal.util.ExceptionUtil;

import java.lang.reflect.Constructor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Starting from jdk 9, we can have more control over ForkJoinPool
 * with its new constructor.
 * <p>
 * This class is for better controlling of life-cycle of pool threads.
 */
class Java9FJPSupplier extends AbstractFJPSupplier {

    private static final int KEEP_ALIVE_SECONDS = 30;
    private static final Class[] CONSTRUCTOR_ARGUMENT_TYPES
            = new Class[]{int.class, ForkJoinPool.ForkJoinWorkerThreadFactory.class,
            Thread.UncaughtExceptionHandler.class, boolean.class,
            int.class, int.class, int.class, Predicate.class, long.class, TimeUnit.class};

    @Override
    ForkJoinPool newJDKSpecificFJP(int parallelism, int maxPoolSize, int minRunnable,
                                   ForkJoinPool.ForkJoinWorkerThreadFactory factory,
                                   Thread.UncaughtExceptionHandler handler, boolean asyncMode) {
        try {
            Class<?> fjpClass = Class.forName("java.util.concurrent.ForkJoinPool");
            Constructor<?> constructor = fjpClass.getConstructor(CONSTRUCTOR_ARGUMENT_TYPES);
            return (ForkJoinPool) constructor.newInstance(parallelism, factory,
                    handler, asyncMode, 0, maxPoolSize, minRunnable,
                    (Predicate<? super ForkJoinPool>) pool -> true, KEEP_ALIVE_SECONDS,
                    TimeUnit.SECONDS);

        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
