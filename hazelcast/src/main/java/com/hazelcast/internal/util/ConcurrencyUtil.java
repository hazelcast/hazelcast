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

package com.hazelcast.internal.util;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Predicate;

/**
 * Utility methods to getOrPutSynchronized and getOrPutIfAbsent in a thread safe way
 * from a {@link ConcurrentMap} with a {@link ConstructorFunction}.
 */
public final class ConcurrencyUtil {

    /**
     * The Caller runs executor is an Executor that executes the task on the calling thread.
     * This is useful when an Executor is required, but offloading to a different thread
     * is very costly and it is faster to run on the calling thread.
     */
    public static final Executor CALLER_RUNS = new Executor() {
        @Override
        public void execute(Runnable command) {
            command.run();
        }

        @Override
        public String toString() {
            return "CALLER_RUNS";
        }
    };

    private static final String PROP_PARALLELISM
            = "hazelcast.internal.default.async.executor.parallelism";
    private static final String PROP_MAX_POOL_SIZE
            = "hazelcast.internal.default.async.executor.maxPoolSize";
    private static final String PROP_MIN_RUNNABLE
            = "hazelcast.internal.default.async.executor.minRunnable";

    private static Executor defaultAsyncExecutor;

    private ConcurrencyUtil() {
    }

    /**
     * WARNING this method should not be called from static context.
     */
    public static Executor getDefaultAsyncExecutor() {
        return defaultAsyncExecutor;
    }

    /**
     * Used by AbstractHazelcastClassRunner to override default executor.
     */
    public static void setDefaultAsyncExecutor(Executor executor) {
        defaultAsyncExecutor = executor;
    }

    /**
     * Atomically sets the max value.
     * <p>
     * If the current value is larger than the provided value, the call is ignored.
     * So it will not happen that a smaller value will overwrite a larger value.
     */
    public static <E> void setMax(E obj, AtomicLongFieldUpdater<E> updater, long value) {
        for (; ; ) {
            long current = updater.get(obj);
            if (current >= value) {
                return;
            }

            if (updater.compareAndSet(obj, current, value)) {
                return;
            }
        }
    }

    public static boolean setIfEqualOrGreaterThan(AtomicLong oldValue, long newValue) {
        while (true) {
            long local = oldValue.get();
            if (newValue < local) {
                return false;
            }
            if (oldValue.compareAndSet(local, newValue)) {
                return true;
            }
        }
    }

    public static <K, V> V getOrPutSynchronized(ConcurrentMap<K, V> map, K key, final Object mutex,
                                                ConstructorFunction<K, V> func) {
        if (mutex == null) {
            throw new NullPointerException();
        }
        V value = map.get(key);
        if (value == null) {
            synchronized (mutex) {
                value = map.get(key);
                if (value == null) {
                    value = func.createNew(key);
                    map.put(key, value);
                }
            }
        }
        return value;
    }

    public static <K, V> V getOrPutSynchronized(ConcurrentMap<K, V> map, K key, ContextMutexFactory contextMutexFactory,
                                                ConstructorFunction<K, V> func) {
        if (contextMutexFactory == null) {
            throw new NullPointerException();
        }
        V value = map.get(key);
        if (value == null) {
            ContextMutexFactory.Mutex mutex = contextMutexFactory.mutexFor(key);
            try {
                synchronized (mutex) {
                    value = map.get(key);
                    if (value == null) {
                        value = func.createNew(key);
                        map.put(key, value);
                    }
                }
            } finally {
                mutex.close();
            }
        }
        return value;
    }

    public static <K, V> V getOrPutIfAbsent(ConcurrentMap<K, V> map, K key, ConstructorFunction<K, V> func) {
        V value = map.get(key);
        if (value == null) {
            value = func.createNew(key);
            V current = map.putIfAbsent(key, value);
            value = current == null ? value : current;
        }
        return value;
    }

    private static ForkJoinPool createDefaultAsyncExecutor() {
        ForkJoinPool.ForkJoinWorkerThreadFactory forkJoinWorkerThreadFactory
                = getForkJoinWorkerThreadFactory();
        PrivilegedAction<ForkJoinPool> privilegedAction = () -> {
            Thread.UncaughtExceptionHandler handler = getUncaughtExceptionHandler();
            int parallelism = getParallelism();
            // For FIFO, set asyncMode to true
            return new ForkJoinPool(parallelism, forkJoinWorkerThreadFactory, handler, true);
        };
        return AccessController.doPrivileged(privilegedAction);
    }

    private static ForkJoinPool.ForkJoinWorkerThreadFactory getForkJoinWorkerThreadFactory() {
        return pool -> {
            PrivilegedAction<ForkJoinWorkerThread> privilegedAction
                    = () -> new HzForkJoinWorkerThread(pool);
            return AccessController.doPrivileged(privilegedAction);
        };
    }

    private static Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return new Thread.UncaughtExceptionHandler() {
            ILogger logger = Logger.getLogger(ConcurrencyUtil.class);

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.severe(t + " had an unexpected exception", e);
            }
        };
    }

    private static int getParallelism() {
        String parallelismFromSystemProp = System.getProperty(PROP_PARALLELISM);
        if (parallelismFromSystemProp != null) {
            return Integer.parseInt(parallelismFromSystemProp);
        }
        return Runtime.getRuntime().availableProcessors();
    }

    private static class HzForkJoinWorkerThread extends ForkJoinWorkerThread {
        protected HzForkJoinWorkerThread(ForkJoinPool pool) {
            super(pool);
            this.setContextClassLoader(ForkJoinPool.class.getClassLoader());
        }
    }

    private static final Class[] CONSTRUCTOR_ARGUMENT_TYPES
            = new Class[]{int.class, ForkJoinPool.ForkJoinWorkerThreadFactory.class,
            Thread.UncaughtExceptionHandler.class, boolean.class,
            int.class, int.class, int.class, Predicate.class, long.class, TimeUnit.class};

    static final Class<?> FJP_CLASS;

    static {
        if (JavaVersion.isAtLeast(JavaVersion.JAVA_9)) {
            try {
                FJP_CLASS = Class.forName("java.util.concurrent.ForkJoinPool");
                Constructor<?> constructor = FJP_CLASS.getConstructor(CONSTRUCTOR_ARGUMENT_TYPES);
                constructor.setAccessible(true);

                int parallelism = getParallelism();
                int maxPoolSize = getMaxPoolSize(parallelism);
                int minRunnable = getMinRunnable(parallelism);

                ForkJoinPool.ForkJoinWorkerThreadFactory forkJoinWorkerThreadFactory
                        = getForkJoinWorkerThreadFactory();
                Thread.UncaughtExceptionHandler uncaughtExceptionHandler = getUncaughtExceptionHandler();

                defaultAsyncExecutor = (Executor) constructor.newInstance(parallelism, forkJoinWorkerThreadFactory,
                        uncaughtExceptionHandler, true, 0, maxPoolSize, minRunnable,
                        (Predicate<? super ForkJoinPool>) pool -> true, 30, TimeUnit.SECONDS);

            } catch (ClassNotFoundException e) {
                throw new RuntimeException();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else {
            FJP_CLASS = null;
            defaultAsyncExecutor = getDefaultAsyncExecutor();
        }
    }

    private static int getMinRunnable(int parallelism) {
        String fromSystemProp = System.getProperty(PROP_MIN_RUNNABLE);
        if (fromSystemProp != null) {
            return Integer.parseInt(fromSystemProp);
        }
        return Integer.max(parallelism / 2, 1);
    }

    private static int getMaxPoolSize(int parallelism) {
        String fromSystemProp = System.getProperty(PROP_MAX_POOL_SIZE);
        if (fromSystemProp != null) {
            return Integer.parseInt(fromSystemProp);
        }

        return Integer.max(parallelism, 256);

//        int maxPoolSize;
//        if (maxPoolSizeValue != null) {
//            maxPoolSize = Integer.parseInt(maxPoolSizeValue);
//            parallelism = Integer.min(parallelism, maxPoolSize);
//        } else {
//            maxPoolSize = Integer.max(parallelism, 256);
//        }
//        return parallelism;
    }
}
