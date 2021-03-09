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

import org.junit.runners.model.RunnerScheduler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.hazelcast.test.HazelcastParallelClassRunner.getDefaultMaxThreads;

/**
 * Runner scheduler to run tests in parallel.
 */
public class HzRunnerScheduler implements RunnerScheduler {

    private static final ExecutorService POOL
            = Executors.newWorkStealingPool(getDefaultMaxThreads());

    private final List<Future<?>> tests = new LinkedList<>();
    private Properties currentSystemProperties;

    @Override
    public void schedule(Runnable test) {
        currentSystemProperties = System.getProperties();
        System.setProperties(new ThreadLocalProperties(currentSystemProperties));

        tests.add(POOL.submit(test));
    }

    @Override
    public void finished() {
        try {
            List<Throwable> throwables = new ArrayList<>();

            for (Future<?> test : tests) {
                try {
                    test.get();
                } catch (Throwable t) {
                    throwables.add(t);
                }
            }

            if (!throwables.isEmpty()) {
                throw createRuntimeException(throwables);
            }
        } finally {
            System.setProperties(currentSystemProperties);
        }
    }

    private static RuntimeException createRuntimeException(List<Throwable> throwables) {
        return new RuntimeException() {
            @Override
            public String getMessage() {
                StringBuilder msg = new StringBuilder();
                for (Throwable throwable : throwables) {
                    msg.append("\n\t-------exception-separator--------\n\n\t");
                    StringWriter sw = new StringWriter();
                    throwable.printStackTrace(new PrintWriter(sw));
                    msg.append(sw.toString());
                    msg.append("\n\t-------exception-separator--------\n");
                }
                return msg.toString();
            }
        };
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
