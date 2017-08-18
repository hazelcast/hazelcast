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
import static java.lang.Thread.currentThread;

public class HazelcastStarter {

    public static final File WORKING_DIRECTORY = Files.createTempDir();

    // Cache downloaded files & classloader used to load their classes per version string
    private static final ConcurrentMap<String, HazelcastVersionClassloaderFuture> loadedVersions =
            new ConcurrentHashMap<String, HazelcastVersionClassloaderFuture>();

    /**
     * Start a new open source {@link HazelcastInstance} of given version.
     *
     * @param version the version string e.g. "3.8". Must correspond to a released version available either in the
     *                local maven repository or maven central.
     * @return        a {@link HazelcastInstance} proxying the started Hazelcast instance.
     */
    public static HazelcastInstance newHazelcastInstance(String version) {
        return newHazelcastInstance(version, null, false);
    }

    /**
     * Start a new open source or enterprise edition {@link HazelcastInstance} of given version. Since no explicit
     * configuration is provided, in order to start enterprise edition a license key must be previously set as a
     * system property.
     *
     * @param version       the version string e.g. "3.8". Must correspond to a released version available either in
     *                      local maven repository or maven central or Hazelcast enterprise repository, in case
     *                      {@code enterprise} is true.
     * @param enterprise    when {@code true}, start Hazelcast enterprise edition, otherwise open source
     * @return              a {@link HazelcastInstance} proxying the started Hazelcast instance.
     */
    public static HazelcastInstance newHazelcastInstance(String version, boolean enterprise) {
        return newHazelcastInstance(version, null, enterprise);
    }

    /**
     * Start a new {@link HazelcastInstance} of the given {@code version} configured with the given {@code Config},
     * open source or enterprise edition.
     *
     * @param version        the version string e.g. "3.8". Must correspond to a released version available either in
     *                       local maven repository or maven central or Hazelcast enterprise repository, in case
     *                       {@code enterprise} is true.
     * @param configTemplate configuration object to clone on the target HazelcastInstance. If {@code null}, default
     *                       configuration is assumed.
     * @param enterprise     when {@code true}, start Hazelcast enterprise edition, otherwise open source
     * @return               a {@link HazelcastInstance} proxying the started Hazelcast instance.
     */
    public static HazelcastInstance newHazelcastInstance(String version, Config configTemplate, boolean enterprise) {
        HazelcastAPIDelegatingClassloader versionClassLoader = getTargetVersionClassloader(version, enterprise,
                configTemplate == null ? null : configTemplate.getClassLoader());

        ClassLoader contextClassLoader = currentThread().getContextClassLoader();
        currentThread().setContextClassLoader(null);
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
                currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    /**
     * Obtain a {@link HazelcastAPIDelegatingClassloader} with the given version's binaries in its classpath.
     * Classloaders are cached, so requesting the classloader for a given version multiple times will return the
     * same instance.
     *
     * @param version   the target Hazelcast version e.g. "3.8.1", must be a published release version.
     * @param configClassLoader class loader given via config
     * @return          a classloader with given version's artifacts in its classpath
     */
    public static HazelcastAPIDelegatingClassloader getTargetVersionClassloader(String version, boolean enterprise, ClassLoader configClassLoader) {
        String versionSpec = versionSpec(version, enterprise);
        HazelcastAPIDelegatingClassloader versionClassLoader = null;
        HazelcastVersionClassloaderFuture future = loadedVersions.get(versionSpec);

        if (future != null) {
            versionClassLoader = future.get();
            return versionClassLoader;
        }

        future = new HazelcastVersionClassloaderFuture(version, enterprise, configClassLoader);
        HazelcastVersionClassloaderFuture found = loadedVersions.putIfAbsent(versionSpec, future);

        if (found != null) {
            versionClassLoader = found.get();
        }

        if (versionClassLoader == null) {
            try {
                versionClassLoader = future.get();
            } catch (Throwable t) {
                loadedVersions.remove(versionSpec, future);
                throw rethrow(t);
            }
        }
        return versionClassLoader;
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

    public static HazelcastInstance newHazelcastClient(String version, boolean enterprise) {
        HazelcastAPIDelegatingClassloader classloader = getTargetVersionClassloader(version, enterprise, null);
        ClassLoader contextClassLoader = currentThread().getContextClassLoader();
        currentThread().setContextClassLoader(null);
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
                currentThread().setContextClassLoader(contextClassLoader);
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

    // Create temporary directory for downloaded artifacts. {@code versionSpec} should include both version
    // and enterprise edition indication. Example values "3.8-EE", "3.8.1".
    private static File getOrCreateVersionVersionDirectory(String versionSpec) {
        File workingDir = WORKING_DIRECTORY;
        if (!workingDir.isDirectory() || !workingDir.exists()) {
            throw new GuardianException("Working directory " + workingDir + " does not exist.");
        }

        File versionDir = new File(WORKING_DIRECTORY, versionSpec);
        versionDir.mkdir();
        return versionDir;
    }

    private static class HazelcastVersionClassloaderFuture {
        private final String version;
        private final boolean enterprise;
        private final ClassLoader configClassLoader;
        private HazelcastAPIDelegatingClassloader classLoader;

        HazelcastVersionClassloaderFuture(String version, boolean enterprise, ClassLoader configClassLoader) {
            this.version = version;
            this.enterprise = enterprise;
            this.configClassLoader = configClassLoader;
        }

        public HazelcastAPIDelegatingClassloader get() {
            if (classLoader != null) {
                return classLoader;
            }

            synchronized (this) {
                File versionDir = getOrCreateVersionVersionDirectory(versionSpec(version, enterprise));
                File[] files = HazelcastVersionLocator.locateVersion(version, versionDir, enterprise);
                URL[] urls = fileIntoUrls(files);
                ClassLoader parentClassloader = HazelcastStarter.class.getClassLoader();
                if (configClassLoader != null) {
                    parentClassloader = configClassLoader;
                }
                classLoader = new HazelcastAPIDelegatingClassloader(urls, parentClassloader);
                return classLoader;
            }
        }
    }

    private static final String versionSpec(String version, boolean enterprise) {
        return enterprise ? version + "-EE" : version;
    }
}
