/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * JUnit 5 extension counterpart of {@link HazelcastParallelClassRunner}.
 * <p>
 * Installs a {@link ThreadLocalProperties} instance at the beginning of a test class
 * execution so that each test thread gets its own copy of system properties, preventing
 * parallel tests from interfering with each other via {@link System#setProperty}.
 * <p>
 * It should be used via {@link ParallelTest}:
 * <pre>{@code
 * @ParallelTest
 * class MyTest { ... }
 * }</pre>
 */
public class HazelcastParallelTestExtension extends AbstractHazelcastExtension
        implements BeforeAllCallback, AfterAllCallback {

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(HazelcastParallelTestExtension.class);
    private static final String SAVED_PROPS_KEY = "savedSystemProperties";

    @Override
    public void beforeAll(ExtensionContext context) {
        // save the current system properties before the test class runs
        Properties currentSystemProperties = System.getProperties();
        context.getStore(NAMESPACE).put(SAVED_PROPS_KEY, currentSystemProperties);
        // use thread-local system properties so parallel tests don't affect each other
        System.setProperties(new ThreadLocalProperties(currentSystemProperties));
    }

    @Override
    public void afterAll(@NonNull ExtensionContext context) throws Exception {
        try {
            super.afterAll(context);
        } finally {
            // restore the system properties after all tests in the class complete
            Properties savedProperties = context.getStore(NAMESPACE)
                    .remove(SAVED_PROPS_KEY, Properties.class);
            if (savedProperties != null) {
                System.setProperties(savedProperties);
            }
        }
    }

    /**
     * A {@link Properties} subclass that provides each thread with its own isolated copy
     * of the properties, initialized lazily from the global properties passed at construction.
     * This mirrors the inner class in {@link HazelcastParallelClassRunner}.
     */
    private static final class ThreadLocalProperties extends Properties {

        private final Properties globalProperties;
        private final ThreadLocal<Properties> localProperties = new InheritableThreadLocal<>();

        private ThreadLocalProperties(Properties properties) {
            this.globalProperties = properties;
        }

        private Properties init(Properties properties) {
            properties.putAll(globalProperties);
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
        public @NonNull Set<Object> keySet() {
            return getThreadLocal().keySet();
        }

        @Override
        public @NonNull Set<Map.Entry<Object, Object>> entrySet() {
            return getThreadLocal().entrySet();
        }

        @Override
        public @NonNull Collection<Object> values() {
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

        @SuppressWarnings("deprecation")
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
