/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.test.annotation.ConfigureParallelRunnerWith;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.lang.reflect.Constructor;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;

/**
 * Runs the test methods in parallel with multiple threads.
 */
public class HazelcastParallelClassRunner extends AbstractHazelcastClassRunner {

    private static final boolean SPAWN_MULTIPLE_THREADS = TestEnvironment.isMockNetwork();
    private static final int DEFAULT_MAX_THREADS = getDefaultMaxThreads();

    static {
        boolean multipleJVM = Boolean.getBoolean("multipleJVM");
        if (multipleJVM) {
            // decrease the amount of resources used when running in multiple JVM
            RuntimeAvailableProcessors.overrideDefault(min(getRuntime().availableProcessors(), 8));
        }
    }

    static int getDefaultMaxThreads() {
        int cpuWorkers = max(getRuntime().availableProcessors(), 8);
        //the parallel profile can spawn multiple JVMs
        boolean multipleJVM = Boolean.getBoolean("multipleJVM");
        if (multipleJVM) {
            // when running tests in multiple JVMs in parallel then we want to put a cap
            // on parallelism inside each JVM. otherwise it's easy to use too much resource
            // and the test duration is actually longer and not shorter.
            cpuWorkers = min(4, cpuWorkers);
        }
        return cpuWorkers;
    }

    public HazelcastParallelClassRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
        setScheduler(new HzRunnerScheduler());
    }

    public HazelcastParallelClassRunner(Class<?> clazz, Object[] parameters, String name) throws InitializationError {
        super(clazz, parameters, name);
        setScheduler(new HzRunnerScheduler());
    }

    public static int getMaxThreads(Class<?> clazz) throws InitializationError {
        if (!SPAWN_MULTIPLE_THREADS) {
            return 1;
        }

        ConfigureParallelRunnerWith annotation = clazz.getAnnotation(ConfigureParallelRunnerWith.class);
        if (annotation != null) {
            try {
                Class<? extends ParallelRunnerOptions> optionsClass = annotation.value();
                Constructor constructor = optionsClass.getConstructor();
                ParallelRunnerOptions parallelRunnerOptions = (ParallelRunnerOptions) constructor.newInstance();
                return min(parallelRunnerOptions.maxParallelTests(), DEFAULT_MAX_THREADS);
            } catch (Exception e) {
                throw new InitializationError(e);
            }
        } else {
            return DEFAULT_MAX_THREADS;
        }
    }

    @Override
    protected void runChild(final FrameworkMethod method, final RunNotifier notifier) {
        new TestRunner(method, notifier).run();
    }

    private class TestRunner implements Runnable {

        private final FrameworkMethod method;
        private final RunNotifier notifier;

        TestRunner(final FrameworkMethod method, final RunNotifier notifier) {
            this.method = method;
            this.notifier = notifier;
        }

        @Override
        public void run() {
            String testName = testName(method);
            setThreadLocalTestMethodName(testName);
            try {
                long start = System.currentTimeMillis();
                System.out.println("Started Running Test: " + testName);
                HazelcastParallelClassRunner.super.runChild(method, notifier);
                float took = (float) (System.currentTimeMillis() - start) / 1000;
                System.out.println(format("Finished Running Test: %s in %.3f seconds.", testName, took));
            } finally {
                removeThreadLocalTestMethodName();
            }
        }
    }
}
