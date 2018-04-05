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

package com.hazelcast.test.starter;

import com.hazelcast.internal.usercodedeployment.impl.ClassloadingMutexProvider;
import com.hazelcast.util.FilteringClassLoader;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.nio.IOUtil.toByteArray;
import static com.hazelcast.test.compatibility.SamplingSerializationService.isTestClass;

/**
 * Classloader which delegates to its parent except when the fully qualified name of the class starts with
 * "com.hazelcast". In this case:
 * <ul>
 * <li>if the class is a test class, then locate its bytes from the parent classloader but load it as a new class
 * in the target class loader. This way user objects implemented in test classpath are loaded on the target classloader
 * therefore implement the appropriate loaded class for any Hazelcast interfaces they implement (eg EntryListener,
 * Predicate etc).</li>
 * <li>otherwise load the requested class from the URLs given to this classloader as constructor argument.</li>
 * </ul>
 */
public class HazelcastAPIDelegatingClassloader extends URLClassLoader {

    static final Set<String> DELEGATION_WHITE_LIST;

    private ClassloadingMutexProvider mutexFactory = new ClassloadingMutexProvider();
    private ClassLoader parent;

    static {
        Set<String> alwaysDelegateWhiteList = new HashSet<String>();
        alwaysDelegateWhiteList.add("com.hazelcast.test.starter.ProxyInvocationHandler");
        alwaysDelegateWhiteList.add("com.hazelcast.test.starter.HazelcastAPIDelegatingClassloader");
        DELEGATION_WHITE_LIST = Collections.unmodifiableSet(alwaysDelegateWhiteList);
    }

    public HazelcastAPIDelegatingClassloader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.parent = parent;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        Utils.debug("Calling getResource with " + name);
        checkResourceExcluded(name);
        if (name.contains("hazelcast")) {
            return findResources(name);
        }
        return super.getResources(name);
    }

    @Override
    public URL getResource(String name) {
        Utils.debug("Getting resource " + name);
        checkResourceExcluded(name);
        if (name.contains("hazelcast")) {
            return findResource(name);
        }
        return super.getResource(name);
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        checkResourceExcluded(name);
        return super.getResourceAsStream(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        checkExcluded(name);
        if (shouldDelegate(name)) {
            return super.loadClass(name, resolve);
        } else {
            Closeable classMutex = mutexFactory.getMutexForClass(name);
            try {
                synchronized (classMutex) {
                    Class<?> loadedClass = findLoadedClass(name);
                    if (loadedClass == null) {
                        // locate test class' bytes in the current codebase but load the class in this classloader
                        // so that the test class implements interfaces from the old Hazelcast version
                        // eg. EntryListener's, EntryProcessor's etc.
                        if (isHazelcastTestClass(name)) {
                            loadedClass = findClassInParentURLs(name);
                        }
                        if (loadedClass == null) {
                            loadedClass = findClass(name);
                        }
                    }
                    //at this point it's always non-null.
                    if (resolve) {
                        resolveClass(loadedClass);
                    }
                    return loadedClass;
                }
            } finally {
                closeResource(classMutex);
            }
        }
    }

    /**
     * Attempts to locate a class' bytes as a resource in parent classpath, then loads the class in this classloader.
     *
     * @return
     */
    private Class<?> findClassInParentURLs(final String name) {
        String classFilePath = name.replaceAll("\\.", "/").concat(".class");
        InputStream classInputStream = getParent().getResourceAsStream(classFilePath);
        if (classInputStream != null) {
            byte[] classBytes = null;
            try {
                classBytes = toByteArray(classInputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (classBytes != null) {
                Class<?> klass = this.defineClass(name, classBytes, 0, classBytes.length);
                return klass;
            }
        }
        return null;
    }

    // delegate to parent if class is not under com.hazelcast package or if class is ProxyInvocationHandler itself.
    private boolean shouldDelegate(String name) {
        if (name.startsWith("usercodedeployment")) {
            return false;
        }

        if (!name.startsWith("com.hazelcast")) {
            return true;
        }

        if (DELEGATION_WHITE_LIST.contains(name)) {
            return true;
        }

        return false;
    }

    private boolean isHazelcastTestClass(String name) {
        if (name.startsWith("usercodedeployment")) {
            return true;
        }

        if (!name.startsWith("com.hazelcast")) {
            return false;
        }

        if (isTestClass(name)) {
            return true;
        }

        return false;
    }

    private void checkExcluded(String className) throws ClassNotFoundException {
        if (parent instanceof FilteringClassLoader) {
            ((FilteringClassLoader) parent).checkExcluded(className);
        }
    }

    private void checkResourceExcluded(String resourceName) {
        if (parent instanceof FilteringClassLoader) {
            ((FilteringClassLoader) parent).checkResourceExcluded(resourceName);
        }
    }
}
