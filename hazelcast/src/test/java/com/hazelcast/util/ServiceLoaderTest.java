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

package com.hazelcast.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ServiceLoaderTest {

    @Test
    public void selectingSimpleSingleClassLoader() {

        List<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(null);
        assertEquals(1, classLoaders.size());
    }

    @Test
    public void selectingSimpleGivenClassLoader() {

        List<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(new URLClassLoader(new URL[0]));
        assertEquals(2, classLoaders.size());
    }

    @Test
    public void selectingSimpleDifferentThreadContextClassLoader() {

        Thread currentThread = Thread.currentThread();
        ClassLoader tccl = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(new URLClassLoader(new URL[0]));
        List<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(null);
        currentThread.setContextClassLoader(tccl);
        assertEquals(2, classLoaders.size());
    }

    @Test
    public void selectingTcclAndGivenClassLoader() {

        Thread currentThread = Thread.currentThread();
        ClassLoader tccl = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(new URLClassLoader(new URL[0]));
        List<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(new URLClassLoader(new URL[0]));
        currentThread.setContextClassLoader(tccl);
        assertEquals(3, classLoaders.size());
    }

    @Test
    public void selectingSameTcclAndGivenClassLoader() {

        ClassLoader same = new URLClassLoader(new URL[0]);

        Thread currentThread = Thread.currentThread();
        ClassLoader tccl = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(same);
        List<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(same);
        currentThread.setContextClassLoader(tccl);
        assertEquals(2, classLoaders.size());
    }

    @Test
    public void loadServicesSingleClassLoader() {

        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        Set<ServiceLoaderTestInterface> implementations = new HashSet<ServiceLoaderTestInterface>();
        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, null);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        assertEquals(1, implementations.size());
    }

    @Test
    public void loadServicesSimpleGivenClassLoader() {

        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        ClassLoader given = new URLClassLoader(new URL[0]);

        Set<ServiceLoaderTestInterface> implementations = new HashSet<ServiceLoaderTestInterface>();
        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, given);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        assertEquals(1, implementations.size());
    }

    @Test
    public void loadServicesSimpleDifferentThreadContextClassLoader() {

        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        Thread current = Thread.currentThread();
        ClassLoader tccl = current.getContextClassLoader();
        current.setContextClassLoader(new URLClassLoader(new URL[0]));

        Set<ServiceLoaderTestInterface> implementations = new HashSet<ServiceLoaderTestInterface>();
        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, null);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        current.setContextClassLoader(tccl);
        assertEquals(1, implementations.size());
    }

    @Test
    public void loadServicesTcclAndGivenClassLoader() {

        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        ClassLoader given = new URLClassLoader(new URL[0]);

        Thread current = Thread.currentThread();
        ClassLoader tccl = current.getContextClassLoader();
        current.setContextClassLoader(new URLClassLoader(new URL[0]));

        Set<ServiceLoaderTestInterface> implementations = new HashSet<ServiceLoaderTestInterface>();
        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, given);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        current.setContextClassLoader(tccl);
        assertEquals(1, implementations.size());
    }

    @Test
    public void loadServicesSameTcclAndGivenClassLoader() {

        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        ClassLoader same = new URLClassLoader(new URL[0]);

        Thread current = Thread.currentThread();
        ClassLoader tccl = current.getContextClassLoader();
        current.setContextClassLoader(same);

        Set<ServiceLoaderTestInterface> implementations = new HashSet<ServiceLoaderTestInterface>();
        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, same);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        current.setContextClassLoader(tccl);
        assertEquals(1, implementations.size());
    }

    @Test
    public void loadServicesGivenClassLoaderWithChildFirstOrdering() throws ClassNotFoundException {
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";
        String className = "com.hazelcast.util.ServiceLoaderTest$ServiceLoaderTestInterface";

        URLClassLoader parentClassLoader = (URLClassLoader)ServiceLoaderTest.class.getClassLoader();
        ClassLoader childClassLoader = new ChildFirstClassLoader(parentClassLoader);
        Class<?> serviceType = childClassLoader.loadClass(className);

        Set<Object> implementations = new HashSet<Object>();
        Iterator<?> iterator = ServiceLoader.iterator(serviceType, factoryId, childClassLoader);
        while (iterator.hasNext()) {
            Object serviceImpl = iterator.next();
            serviceImpl.getClass().isAssignableFrom(serviceType);
            implementations.add(serviceImpl);
        }

        assertEquals(1, implementations.size());
    }

    @Test(expected=NoSuchElementException.class)
    public void iteratorExhaustedTest() {
        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, null);
        try {
            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
            assertFalse(iterator.hasNext());
        }
        catch (NoSuchElementException e) {
            fail("Methods must not throw NoSuchElementException");
        }
        iterator.next();
    }

    @Test(expected=NoSuchElementException.class)
    public void iteratorExhaustedTestNoPrefetching() {
        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, null);
        try {
            assertNotNull(iterator.next());
        }
        catch (NoSuchElementException e) {
            fail("Methods must not throw NoSuchElementException");
        }
        iterator.next();
    }

    @Test
    public void iteratorHasNextDoesNotAdvance() {
        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, null);
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
    }

    public static interface ServiceLoaderTestInterface {
    }

    public static class ServiceLoaderTestInterfaceImpl
            implements ServiceLoaderTestInterface {
    }

    /**
     * This classloader loads any class name starting with com.hazelcast.util itself instead of asking the parent.
     * Other classes are loaded parent first.
     */
    public static class ChildFirstClassLoader extends URLClassLoader {
        private static final Pattern pattern = Pattern.compile("^com\\.hazelcast\\.util\\..*$");
        public ChildFirstClassLoader(URLClassLoader classLoader) {
            super(classLoader.getURLs(), classLoader);
        }

        @Override
        protected Class<?> loadClass(final String name,
            final boolean resolve)
            throws ClassNotFoundException {

            final Matcher matcher = pattern.matcher(name);
            if (matcher.matches()) {
                return loadFromThisClassLoader(name, resolve);
            }
            else {
                return super.loadClass(name, resolve);
            }
        }

        private synchronized Class<?> loadFromThisClassLoader(String name, boolean resolve)
                throws ClassNotFoundException {

            Class<?> c = findLoadedClass(name);
            if (c == null) {
                c = findClass(name);
            }
            if (resolve) {
                resolveClass(c);
            }
            return c;
        }
    }
}
