/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.google.common.io.Files;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.util.ExceptionUtil;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyObjectForStarter;
import static com.hazelcast.test.starter.Utils.rethrow;
import static com.hazelcast.util.ExceptionUtil.rethrow;

public class HazelcastStarter {

    public static final File WORKING_DIRECTORY = Files.createTempDir();

    // Cache downloaded files & classloader used to load their classes per version string
    private static final ConcurrentMap<String, HazelcastVersionClassloaderFuture> loadedVersions =
            new ConcurrentHashMap<String, HazelcastVersionClassloaderFuture>();

    public static HazelcastInstance newHazelcastInstance(String version) {
        return newHazelcastInstance(version, null);
    }

    /**
     * Start a new {@link HazelcastInstance} of the given {@code version}, configured with the given {@code Config}.
     *
     * @param version           Hazelcast version to start; must be a published artifact on maven central
     * @param configTemplate    configuration object to clone on the target HazelcastInstance
     * @return
     */
    public static HazelcastInstance newHazelcastInstance(String version, Config configTemplate) {
        return newHazelcastInstance(version, configTemplate, null);
    }

    public static HazelcastInstance newHazelcastInstance(String version, Config configTemplate,
                                                         NodeContext nodeContextTemplate) {
        HazelcastAPIDelegatingClassloader versionClassLoader = getTargetVersionClassloader(version);

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        try {
            return newHazelcastMemberWithNetwork(configTemplate, versionClassLoader);
        } catch (ClassNotFoundException e) {
            throw rethrow(e);
        } catch (NoSuchMethodException e) {
            throw rethrow(e);
        } catch (IllegalAccessException e) {
            throw rethrow(e);
        } catch (InvocationTargetException e) {
            throw rethrow(e);
        } catch (InstantiationException e) {
            throw rethrow(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    /**
     * Obtain a {@link HazelcastAPIDelegatingClassloader} with the given version's binaries in its classpath.
     * Classloaders are cached, so requesting the classloader for a given version multiple times will return the
     * same instance.
     *
     * @param version   the target Hazelcast version e.g. "3.8.1", must be a published release version.
     * @return          a classloader with given version's artifacts in its classpath
     */
    public static HazelcastAPIDelegatingClassloader getTargetVersionClassloader(String version) {
        HazelcastAPIDelegatingClassloader versionClassLoader = null;
        HazelcastVersionClassloaderFuture future = loadedVersions.get(version);

        if (future != null) {
            versionClassLoader = future.get();
        }

        future = new HazelcastVersionClassloaderFuture(version);
        HazelcastVersionClassloaderFuture found = loadedVersions.putIfAbsent(version, future);

        if (found != null) {
            versionClassLoader = found.get();
        }

        if (versionClassLoader == null) {
            try {
                versionClassLoader = future.get();
            } catch (Throwable t) {
                loadedVersions.remove(version, future);
                throw rethrow(t);
            }
        }
        return versionClassLoader;
    }

    public static InternalSerializationService getTargetVersionSerializationService(String version) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        HazelcastAPIDelegatingClassloader targetClassloader = getTargetVersionClassloader(version);
        Class klass = null;
        try {
            klass = targetClassloader.loadClass("com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder");
            SerializationServiceBuilder targetBuilder = (SerializationServiceBuilder)
                    proxyObjectForStarter(DefaultSerializationServiceBuilder.class.getClassLoader(), klass.newInstance());

            Thread.currentThread().setContextClassLoader(targetClassloader);
            InternalSerializationService targetSerializationService = targetBuilder
                    .setClassLoader(targetClassloader)
                    .setVersion(InternalSerializationService.VERSION_1).build();

            return targetSerializationService;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    private static HazelcastInstance newHazelcastMemberWithNetwork(Config configTemplate,
                                                                   HazelcastAPIDelegatingClassloader classloader)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
            NoSuchMethodException, InvocationTargetException {
        Class<Hazelcast> hazelcastClass = (Class<Hazelcast>) classloader.loadClass("com.hazelcast.core.Hazelcast");
        System.out.println(hazelcastClass + " loaded by " + hazelcastClass.getClassLoader());
        Class<?> configClass = classloader.loadClass("com.hazelcast.config.Config");
        Object config;
        config = getConfig(configTemplate, classloader, configClass);

        Method newHazelcastInstanceMethod = hazelcastClass.getMethod("newHazelcastInstance", configClass);
        Object delegate = newHazelcastInstanceMethod.invoke(null, config);

        return (HazelcastInstance) proxyObjectForStarter(HazelcastStarter.class.getClassLoader(), delegate);
    }

    private static Object getConfig(Config configTemplate, HazelcastAPIDelegatingClassloader classloader,
                                    Class<?> configClass)
            throws InstantiationException, IllegalAccessException, NoSuchMethodException,
            InvocationTargetException, ClassNotFoundException {
        Object config;
        if (configTemplate == null) {
            config = configClass.newInstance();
            Method setClassLoaderMethod = configClass.getMethod("setClassLoader", ClassLoader.class);
            setClassLoaderMethod.invoke(config, classloader);
        } else {
            config = proxyObjectForStarter(classloader, configTemplate);
        }
        return config;
    }

    public static HazelcastInstance newHazelcastClient(String version) {
        File versionDir = getOrCreateVersionVersionDirectory(version);
        File[] files = HazelcastVersionLocator.locateVersion(version, versionDir, true);
        URL[] urls = fileIntoUrls(files);
        ClassLoader parentClassloader = HazelcastStarter.class.getClassLoader();
        HazelcastAPIDelegatingClassloader classloader = new HazelcastAPIDelegatingClassloader(urls, parentClassloader);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        try {
            Class<Hazelcast> hazelcastClass = (Class<Hazelcast>) classloader.loadClass("com.hazelcast.client.HazelcastClient");
            System.out.println(hazelcastClass + " loaded by " + hazelcastClass.getClassLoader());
            Class<?> configClass = classloader.loadClass("com.hazelcast.client.config.ClientConfig");
            Object config = configClass.newInstance();
            Method setClassLoaderMethod = configClass.getMethod("setClassLoader", ClassLoader.class);
            setClassLoaderMethod.invoke(config, classloader);

            Method newHazelcastInstanceMethod = hazelcastClass.getMethod("newHazelcastClient", configClass);
            Object delegate = newHazelcastInstanceMethod.invoke(null, config);
            return (HazelcastInstance) proxyObjectForStarter(HazelcastStarter.class.getClassLoader(), delegate);

        } catch (ClassNotFoundException e) {
            throw rethrow(e);
        } catch (NoSuchMethodException e) {
            throw rethrow(e);
        } catch (IllegalAccessException e) {
            throw rethrow(e);
        } catch (InvocationTargetException e) {
            throw rethrow(e);
        } catch (InstantiationException e) {
            throw rethrow(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    private static URL[] fileIntoUrls(File[] files) {
        URL[] urls = new URL[files.length];
        for (int i = 0; i < files.length; i++) {
            try {
                urls[i] = files[i].toURL();
            } catch (MalformedURLException e) {
                throw rethrow(e);
            }
        }
        return urls;
    }

    private static File getOrCreateVersionVersionDirectory(String version) {
        File workingDir = WORKING_DIRECTORY;
        if (!workingDir.isDirectory() || !workingDir.exists()) {
            throw new GuardianException("Working directory " + workingDir + " does not exist.");
        }

        File versionDir = new File(WORKING_DIRECTORY, version);
        versionDir.mkdir();
        return versionDir;
    }

    private static class HazelcastVersionClassloaderFuture {
        private final String version;

        private HazelcastAPIDelegatingClassloader classLoader;

        HazelcastVersionClassloaderFuture(String version) {
            this.version = version;
        }

        public HazelcastAPIDelegatingClassloader get() {
            if (classLoader != null) {
                return classLoader;
            }

            synchronized (this) {
                File versionDir = getOrCreateVersionVersionDirectory(version);
                File[] files = HazelcastVersionLocator.locateVersion(version, versionDir, true);
                URL[] urls = fileIntoUrls(files);
                ClassLoader parentClassloader = HazelcastStarter.class.getClassLoader();
                classLoader = new HazelcastAPIDelegatingClassloader(urls, parentClassloader);
                return classLoader;
            }
        }
    }
}
