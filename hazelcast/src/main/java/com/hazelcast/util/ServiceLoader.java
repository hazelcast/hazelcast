/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ClassLoaderUtil;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.Preconditions.isNotNull;
import static java.lang.Boolean.getBoolean;

/**
 * Support class for loading Hazelcast services and hooks based on the Java
 * {@link java.util.ServiceLoader} specification, but changed to the fact of
 * class loaders to test for given services to work in multi classloader
 * environments like application or OSGi servers.
 */
public final class ServiceLoader {
    //compatibility flag to re-introduce behaviour from 3.8.0 with classloading fallbacks
    private static final boolean USE_CLASSLOADING_FALLBACK = getBoolean("hazelcast.compat.classloading.hooks.fallback");

    private static final ILogger LOGGER = Logger.getLogger(ServiceLoader.class);

    // see https://github.com/hazelcast/hazelcast/issues/3922
    private static final String IGNORED_GLASSFISH_MAGIC_CLASSLOADER =
            "com.sun.enterprise.v3.server.APIClassLoaderServiceImpl$APIClassLoader";

    private ServiceLoader() {
    }

    public static <T> T load(Class<T> clazz, String factoryId, ClassLoader classLoader) throws Exception {
        Iterator<T> iterator = iterator(clazz, factoryId, classLoader);
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    public static <T> Iterator<T> iterator(Class<T> expectedType, String factoryId, ClassLoader classLoader) throws Exception {
        Iterator<Class<T>> classIterator = classIterator(expectedType, factoryId, classLoader);
        return new NewInstanceIterator<T>(classIterator);
    }

    public static <T> Iterator<Class<T>> classIterator(Class<T> expectedType, String factoryId, ClassLoader classLoader)
            throws Exception {
        Set<ServiceDefinition> serviceDefinitions = getServiceDefinitions(factoryId, classLoader);
        return new ClassIterator<T>(serviceDefinitions, expectedType);
    }

    private static Set<ServiceDefinition> getServiceDefinitions(String factoryId, ClassLoader classLoader) {
        List<ClassLoader> classLoaders = selectClassLoaders(classLoader);

        Set<URLDefinition> factoryUrls = new HashSet<URLDefinition>();
        for (ClassLoader selectedClassLoader : classLoaders) {
            factoryUrls.addAll(collectFactoryUrls(factoryId, selectedClassLoader));
        }

        Set<ServiceDefinition> serviceDefinitions = new HashSet<ServiceDefinition>();
        for (URLDefinition urlDefinition : factoryUrls) {
            serviceDefinitions.addAll(parse(urlDefinition));
        }
        if (serviceDefinitions.isEmpty()) {
            Logger.getLogger(ServiceLoader.class).finest(
                    "Service loader could not load 'META-INF/services/" + factoryId + "'. It may be empty or does not exist.");
        }
        return serviceDefinitions;
    }

    private static Set<URLDefinition> collectFactoryUrls(String factoryId, ClassLoader classLoader) {
        String resourceName = "META-INF/services/" + factoryId;
        try {
            Enumeration<URL> configs = classLoader.getResources(resourceName);

            Set<URLDefinition> urlDefinitions = new HashSet<URLDefinition>();
            while (configs.hasMoreElements()) {
                URL url = configs.nextElement();
                String externalForm = url.toExternalForm()
                                         .replace(" ", "%20")
                                         .replace("^", "%5e");
                URI uri = new URI(externalForm);

                if (!classLoader.getClass().getName().equals(IGNORED_GLASSFISH_MAGIC_CLASSLOADER)) {
                    urlDefinitions.add(new URLDefinition(uri, classLoader));
                }
            }
            return urlDefinitions;

        } catch (Exception e) {
            LOGGER.severe(e);
        }
        return Collections.emptySet();
    }

    private static Set<ServiceDefinition> parse(URLDefinition urlDefinition) {
        try {
            Set<ServiceDefinition> names = new HashSet<ServiceDefinition>();
            BufferedReader r = null;
            try {
                URL url = urlDefinition.uri.toURL();
                r = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
                while (true) {
                    String line = r.readLine();
                    if (line == null) {
                        break;
                    }
                    int comment = line.indexOf('#');
                    if (comment >= 0) {
                        line = line.substring(0, comment);
                    }
                    String name = line.trim();
                    if (name.length() == 0) {
                        continue;
                    }
                    names.add(new ServiceDefinition(name, urlDefinition.classLoader));
                }
            } finally {
                closeResource(r);
            }
            return names;
        } catch (Exception e) {
            LOGGER.severe(e);
        }
        return Collections.emptySet();
    }

    static List<ClassLoader> selectClassLoaders(ClassLoader classLoader) {
        // list prevents reordering!
        List<ClassLoader> classLoaders = new ArrayList<ClassLoader>();

        if (classLoader != null) {
            classLoaders.add(classLoader);
        }

        // check if TCCL is same as given classLoader
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        if (tccl != null && tccl != classLoader) {
            classLoaders.add(tccl);
        }

        // Hazelcast core classLoader
        ClassLoader coreClassLoader = ServiceLoader.class.getClassLoader();
        if (coreClassLoader != classLoader && coreClassLoader != tccl) {
            classLoaders.add(coreClassLoader);
        }

        // Hazelcast client classLoader
        try {
            Class<?> hzClientClass = Class.forName("com.hazelcast.client.HazelcastClient");
            ClassLoader clientClassLoader = hzClientClass.getClassLoader();
            if (clientClassLoader != classLoader && clientClassLoader != tccl && clientClassLoader != coreClassLoader) {
                classLoaders.add(clientClassLoader);
            }
        } catch (ClassNotFoundException ignore) {
            // ignore since we may not have the HazelcastClient in the classpath
            ignore(ignore);
        }

        return classLoaders;
    }

    /**
     * Definition of the internal service based on the classloader that is able to load it
     * and the class name of the service that was found.
     */
    static final class ServiceDefinition {

        private final String className;
        private final ClassLoader classLoader;

        public ServiceDefinition(String className, ClassLoader classLoader) {
            this.className = isNotNull(className, "className");
            this.classLoader = isNotNull(classLoader, "classLoader");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ServiceDefinition that = (ServiceDefinition) o;
            if (!classLoader.equals(that.classLoader)) {
                return false;
            }
            if (!className.equals(that.className)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = className.hashCode();
            result = 31 * result + classLoader.hashCode();
            return result;
        }
    }

    /**
     * This class keeps track of available service definition URLs and
     * the corresponding classloaders.
     */
    private static final class URLDefinition {

        private final URI uri;
        private final ClassLoader classLoader;

        private URLDefinition(URI url, ClassLoader classLoader) {
            this.uri = url;
            this.classLoader = classLoader;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            URLDefinition that = (URLDefinition) o;
            if (uri != null ? !uri.equals(that.uri) : that.uri != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return uri == null ? 0 : uri.hashCode();
        }
    }

    static class NewInstanceIterator<T> implements Iterator<T> {

        private final Iterator<Class<T>> classIterator;

        NewInstanceIterator(Iterator<Class<T>> classIterator) {
            this.classIterator = classIterator;
        }

        @Override
        public boolean hasNext() {
            return classIterator.hasNext();
        }

        @Override
        public T next() {
            Class<T> clazz = classIterator.next();
            try {
                Constructor<T> constructor = clazz.getDeclaredConstructor();
                if (!constructor.isAccessible()) {
                    constructor.setAccessible(true);
                }
                return constructor.newInstance();
            } catch (InstantiationException e) {
                throw new HazelcastException(e);
            } catch (IllegalAccessException e) {
                throw new HazelcastException(e);
            } catch (NoSuchMethodException e) {
                throw new HazelcastException(e);
            } catch (InvocationTargetException e) {
                throw new HazelcastException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Iterates over services. It skips services which implement an interface with the expected name,
     * but loaded by a different classloader.
     * <p>
     * When a service does not implement an interface with expected name then it throws an exception
     *
     * @param <T>
     */
    static class ClassIterator<T> implements Iterator<Class<T>> {

        private final Iterator<ServiceDefinition> iterator;
        private final Class<T> expectedType;
        private Class<T> nextClass;
        private Set<Class<?>> alreadyProvidedClasses = new HashSet<Class<?>>();

        ClassIterator(Set<ServiceDefinition> serviceDefinitions, Class<T> expectedType) {
            iterator = serviceDefinitions.iterator();
            this.expectedType = expectedType;
        }

        @Override
        public boolean hasNext() {
            if (nextClass != null) {
                return true;
            }
            return advance();
        }

        private boolean advance() {
            while (iterator.hasNext()) {
                ServiceDefinition definition = iterator.next();
                String className = definition.className;
                ClassLoader classLoader = definition.classLoader;

                try {
                    Class<?> candidate = loadClass(className, classLoader);
                    if (expectedType.isAssignableFrom(candidate)) {
                        if (!isDuplicate(candidate)) {
                            nextClass = (Class<T>) candidate;
                            return true;
                        }
                    } else {
                        onNonAssignableClass(className, candidate);
                    }
                } catch (ClassNotFoundException e) {
                    onClassNotFoundException(className, classLoader, e);
                }
            }
            return false;
        }

        private boolean isDuplicate(Class<?> candidate) {
            return !alreadyProvidedClasses.add(candidate);
        }

        private Class<?> loadClass(String className, ClassLoader classLoader) throws ClassNotFoundException {
            Class<?> candidate;
            if (USE_CLASSLOADING_FALLBACK) {
                candidate = ClassLoaderUtil.loadClass(classLoader, className);
            } else {
                candidate = classLoader.loadClass(className);
            }
            return candidate;
        }

        private void onClassNotFoundException(String className, ClassLoader classLoader, ClassNotFoundException e) {
            if (className.startsWith("com.hazelcast")) {
                LOGGER.fine("Failed to load " + className + " by " + classLoader
                        + ". This indicates a classloading issue. It can happen in a runtime with "
                        + "a complicated classloading model. (OSGi, Java EE, etc);");
            } else {
                throw new HazelcastException(e);
            }
        }

        private void onNonAssignableClass(String className, Class candidate) {
            if (expectedType.isInterface()) {
                if (ClassLoaderUtil.implementsInterfaceWithSameName(candidate, expectedType)) {
                    // this can happen in application containers - different Hazelcast JARs are loaded
                    // by different classloaders.
                    LOGGER.fine("There appears to be a classloading conflict. "
                            + "Class " + className + " loaded by " + candidate.getClassLoader() + " implements "
                            + expectedType.getName() + " from its own class loader, but it does not implement "
                            + expectedType.getName() + " loaded by " + expectedType.getClassLoader());
                } else {
                    //the class does not implement interface with the expected name.
                    LOGGER.fine("There appears to be a classloading conflict. "
                            + "Class " + className + " loaded by " + candidate.getClassLoader() + " does not "
                            + "implement an interface with name " + expectedType.getName() + " in both class loaders."
                            + "the interface currently loaded by " + expectedType.getClassLoader());
                }
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<T> next() {
            if (nextClass == null) {
                advance();
            }
            if (nextClass == null) {
                throw new NoSuchElementException();
            }
            Class<T> classToReturn = nextClass;
            nextClass = null;
            return classToReturn;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
