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

package com.hazelcast.test;

import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.test.annotation.ConfigureParallelRunnerWith;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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

    private static int getDefaultMaxThreads() {
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

    private final AtomicInteger numThreads = new AtomicInteger(0);
    private final int maxThreads;

    public HazelcastParallelClassRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
        maxThreads = getMaxThreads(clazz);
    }

    public HazelcastParallelClassRunner(Class<?> clazz, Object[] parameters, String name) throws InitializationError {
        super(clazz, parameters, name);
        maxThreads = getMaxThreads(clazz);
    }

    private int getMaxThreads(Class<?> clazz) throws InitializationError {
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
        while (numThreads.get() >= maxThreads) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        numThreads.incrementAndGet();
        new MultithreadedTestRunnerThread(new TestRunner(method, notifier)).start();
    }

    @Override
    protected Statement childrenInvoker(final RunNotifier notifier) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                // save the current system properties
                Properties currentSystemProperties = System.getProperties();
                try {
                    // use thread-local based system properties so parallel tests don't effect each other
                    System.setProperties(new ThreadLocalProperties(currentSystemProperties));
                    HazelcastParallelClassRunner.super.childrenInvoker(notifier).evaluate();
                    // wait for all child threads (tests) to complete
                    while (numThreads.get() > 0) {
                        Thread.sleep(25);
                    }
                } finally {
                    // restore the system properties
                    System.setProperties(currentSystemProperties);
                }
            }
        };
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
                numThreads.decrementAndGet();
                float took = (float) (System.currentTimeMillis() - start) / 1000;
                System.out.println(format("Finished Running Test: %s in %.3f seconds.", testName, took));
            } finally {
                removeThreadLocalTestMethodName();
            }
        }
    }

    @SuppressWarnings({"deprecation"})
    private static final class ThreadLocalProperties extends Properties {

        private final Properties globalProperties;

        private final ThreadLocal<Properties> localProperties = new InheritableThreadLocal<Properties>();

        private ThreadLocalProperties(Properties properties) {
            this.globalProperties = properties;
        }

        private Properties init(Properties properties) {
            for (Map.Entry entry : globalProperties.entrySet()) {
                properties.put(entry.getKey(), entry.getValue());
            }
            return properties;
        }

        private Properties getThreadLocal() {
            Properties properties = localProperties.get();
            if (properties == null) {
                properties = init(new Properties());
                localProperties.set(properties);
            }
            return properties;
        }

        @Override
        public String getProperty(String key) {
            return getThreadLocal().getProperty(key);
        }

        @Override
        public Object setProperty(String key, String value) {
            return getThreadLocal().setProperty(key, value);
        }

        @Override
        public Enumeration<?> propertyNames() {
            return getThreadLocal().propertyNames();
        }

        @Override
        public Set<String> stringPropertyNames() {
            return getThreadLocal().stringPropertyNames();
        }

        @Override
        public int size() {
            return getThreadLocal().size();
        }

        @Override
        public boolean isEmpty() {
            return getThreadLocal().isEmpty();
        }

        @Override
        public Enumeration<Object> keys() {
            return getThreadLocal().keys();
        }

        @Override
        public Enumeration<Object> elements() {
            return getThreadLocal().elements();
        }

        @Override
        public boolean contains(Object value) {
            return getThreadLocal().contains(value);
        }

        @Override
        public boolean containsValue(Object value) {
            return getThreadLocal().containsValue(value);
        }

        @Override
        public boolean containsKey(Object key) {
            return getThreadLocal().containsKey(key);
        }

        @Override
        public Object get(Object key) {
            return getThreadLocal().get(key);
        }

        @Override
        public Object put(Object key, Object value) {
            return getThreadLocal().put(key, value);
        }

        @Override
        public Object remove(Object key) {
            return getThreadLocal().remove(key);
        }

        @Override
        public void putAll(Map<?, ?> t) {
            getThreadLocal().putAll(t);
        }

        @Override
        public void clear() {
            getThreadLocal().clear();
        }

        @Override
        public Set<Object> keySet() {
            return getThreadLocal().keySet();
        }

        @Override
        public Set<Map.Entry<Object, Object>> entrySet() {
            return getThreadLocal().entrySet();
        }

        @Override
        public Collection<Object> values() {
            return getThreadLocal().values();
        }

        @Override
        public void load(Reader reader) throws IOException {
            getThreadLocal().load(reader);
        }

        @Override
        public void load(InputStream inStream) throws IOException {
            getThreadLocal().load(inStream);
        }

        @Override
        public void save(OutputStream out, String comments) {
            getThreadLocal().save(out, comments);
        }

        @Override
        public void store(Writer writer, String comments) throws IOException {
            getThreadLocal().store(writer, comments);
        }

        @Override
        public void store(OutputStream out, String comments) throws IOException {
            getThreadLocal().store(out, comments);
        }

        @Override
        public void loadFromXML(InputStream in) throws IOException {
            getThreadLocal().loadFromXML(in);
        }

        @Override
        public void storeToXML(OutputStream os, String comment) throws IOException {
            getThreadLocal().storeToXML(os, comment);
        }

        @Override
        public void storeToXML(OutputStream os, String comment, String encoding) throws IOException {
            getThreadLocal().storeToXML(os, comment, encoding);
        }

        @Override
        public String getProperty(String key, String defaultValue) {
            return getThreadLocal().getProperty(key, defaultValue);
        }

        @Override
        public void list(PrintStream out) {
            getThreadLocal().list(out);
        }

        @Override
        public void list(PrintWriter out) {
            getThreadLocal().list(out);
        }
    }
}
