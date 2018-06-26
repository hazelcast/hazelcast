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
import static com.hazelcast.test.starter.HazelcastStarterUtils.rethrowGuardianException;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.Thread.currentThread;

public class HazelcastStarter {

    /**
     * Starts a new open source {@link HazelcastInstance} of the given version.
     *
     * @param version the version string e.g. "3.8". Must correspond to a released version available either in the
     *                local maven repository or maven central.
     * @return a {@link HazelcastInstance} proxying the started Hazelcast instance.
     */
    public static HazelcastInstance newHazelcastInstance(String version) {
        return newHazelcastInstance(version, null, false);
    }

    /**
     * Caches downloaded files & classloader used to load their classes per version string.
     */
    private static final ConcurrentMap<String, HazelcastVersionClassloaderFuture> LOADED_VERSIONS =
            new ConcurrentHashMap<String, HazelcastVersionClassloaderFuture>();

    private static final File WORKING_DIRECTORY = Files.createTempDir();

    /**
     * Starts a new open source or enterprise edition {@link HazelcastInstance} of the given version. Since no
     * explicit configuration is provided, a license key must be previously set as a system property to start
     * Hazelcast enterprise edition.
     *
     * @param version    the version string e.g. "3.8". Must correspond to a released version available either in
     *                   local maven repository or maven central or Hazelcast enterprise repository, in case
     *                   {@code enterprise} is true.
     * @param enterprise when {@code true}, start Hazelcast enterprise edition, otherwise open source
     * @return a {@link HazelcastInstance} proxying the started Hazelcast instance.
     */
    public static HazelcastInstance newHazelcastInstance(String version, boolean enterprise) {
        return newHazelcastInstance(version, null, enterprise);
    }

    /**
     * Starts a new {@link HazelcastInstance} of the given {@code version} configured with the given {@code Config},
     * open source or enterprise edition.
     *
     * @param version        the version string e.g. "3.8". Must correspond to a released version available either in
     *                       local maven repository or maven central or Hazelcast enterprise repository, in case
     *                       {@code enterprise} is {@code true}
     * @param configTemplate configuration object to clone on the target HazelcastInstance. If {@code null}, default
     *                       configuration is assumed
     * @param enterprise     when {@code true}, start Hazelcast enterprise edition, otherwise open source
     * @return a {@link HazelcastInstance} proxying the started Hazelcast instance.
     */
    public static HazelcastInstance newHazelcastInstance(String version, Config configTemplate, boolean enterprise) {
        HazelcastAPIDelegatingClassloader versionClassLoader = getTargetVersionClassloader(version, enterprise,
                configTemplate == null ? null : configTemplate.getClassLoader());

        ClassLoader contextClassLoader = currentThread().getContextClassLoader();
        currentThread().setContextClassLoader(null);
        try {
            return newHazelcastMemberWithNetwork(configTemplate, versionClassLoader);
        } catch (ClassNotFoundException e) {
            throw rethrowGuardianException(e);
        } catch (NoSuchMethodException e) {
            throw rethrowGuardianException(e);
        } catch (IllegalAccessException e) {
            throw rethrowGuardianException(e);
        } catch (InvocationTargetException e) {
            throw rethrowGuardianException(e);
        } catch (InstantiationException e) {
            throw rethrowGuardianException(e);
        } finally {
            if (contextClassLoader != null) {
                currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    /**
     * Obtains a {@link HazelcastAPIDelegatingClassloader} with the given version's binaries in its classpath.
     * Classloaders are cached, so requesting the classloader for a given version multiple times will return the
     * same instance.
     *
     * @param version           the target Hazelcast version e.g. "3.8.1", must be a published release version
     * @param configClassLoader class loader given via config
     * @return a classloader with given version's artifacts in its classpath
     */
    public static HazelcastAPIDelegatingClassloader getTargetVersionClassloader(String version, boolean enterprise,
                                                                                ClassLoader configClassLoader) {
        HazelcastVersionClassloaderFuture future;
        if (configClassLoader != null) {
            // when a custom ClassLoader should be the parent of the target version ClassLoader,
            // do not use the ClassLoader cache
            future = new HazelcastVersionClassloaderFuture(version, enterprise, configClassLoader);
            return future.get();
        }

        String versionSpec = versionSpec(version, enterprise);
        future = LOADED_VERSIONS.get(versionSpec);
        if (future != null) {
            return future.get();
        }

        future = new HazelcastVersionClassloaderFuture(version, enterprise, null);
        HazelcastVersionClassloaderFuture found = LOADED_VERSIONS.putIfAbsent(versionSpec, future);
        if (found != null) {
            HazelcastAPIDelegatingClassloader versionClassLoader = found.get();
            if (versionClassLoader != null) {
                return versionClassLoader;
            }
        }

        try {
            return future.get();
        } catch (Throwable t) {
            LOADED_VERSIONS.remove(versionSpec, future);
            throw rethrow(t);
        }
    }

    private static HazelcastInstance newHazelcastMemberWithNetwork(Config configTemplate,
                                                                   HazelcastAPIDelegatingClassloader classloader)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
            NoSuchMethodException, InvocationTargetException {
        Class<Hazelcast> hazelcastClass = (Class<Hazelcast>) classloader.loadClass("com.hazelcast.core.Hazelcast");
        System.out.println(hazelcastClass + " loaded by " + hazelcastClass.getClassLoader());
        Class<?> configClass = classloader.loadClass("com.hazelcast.config.Config");
        Object config = getConfig(configTemplate, classloader, configClass);

        Method newHazelcastInstanceMethod = hazelcastClass.getMethod("newHazelcastInstance", configClass);
        Object delegate = newHazelcastInstanceMethod.invoke(null, config);

        return (HazelcastInstance) proxyObjectForStarter(HazelcastStarter.class.getClassLoader(), delegate);
    }

    public static Object getConfig(Object configTemplate, HazelcastAPIDelegatingClassloader classloader, Class<?> configClass)
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

    private static String versionSpec(String version, boolean enterprise) {
        return enterprise ? version + "-EE" : version;
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
                File versionDir = getOrCreateVersionDirectory(versionSpec(version, enterprise));
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

        private static URL[] fileIntoUrls(File[] files) {
            URL[] urls = new URL[files.length];
            for (int i = 0; i < files.length; i++) {
                try {
                    urls[i] = files[i].toURI().toURL();
                } catch (MalformedURLException e) {
                    throw rethrowGuardianException(e);
                }
            }
            return urls;
        }
    }

    /**
     * Creates a temporary directory for downloaded artifacts.
     *
     * @param versionSpec the version specification, which should include both version and enterprise edition indication,
     *                    e.g. "3.8-EE", "3.8.1"
     */
    public static File getOrCreateVersionDirectory(String versionSpec) {
        File workingDir = WORKING_DIRECTORY;
        if (!workingDir.isDirectory() || !workingDir.exists()) {
            throw new GuardianException("Working directory " + workingDir.getAbsolutePath() + " does not exist.");
        }

        File versionDir = new File(WORKING_DIRECTORY, versionSpec);
        // the versionDir may already exist when the same Hazelcast version artifacts were previously downloaded
        if (!versionDir.exists() && !versionDir.mkdir()) {
            throw new GuardianException("Version directory " + versionDir.getAbsolutePath() + " could not be created.");
        }
        return versionDir;
    }
}
