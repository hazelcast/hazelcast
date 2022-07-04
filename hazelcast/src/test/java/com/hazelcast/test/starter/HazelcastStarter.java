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

package com.hazelcast.test.starter;

import com.google.common.io.Files;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.test.starter.answer.HazelcastInstanceImplAnswer;
import com.hazelcast.test.starter.answer.NodeAnswer;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyObjectForStarter;
import static com.hazelcast.test.starter.HazelcastStarterUtils.debug;
import static com.hazelcast.test.starter.HazelcastStarterUtils.rethrowGuardianException;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;
import static com.hazelcast.test.starter.ReflectionUtils.setFieldValueReflectively;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static java.lang.Thread.currentThread;
import static java.lang.reflect.Proxy.isProxyClass;
import static org.mockito.Mockito.mock;

public class HazelcastStarter {

    /**
     * Caches downloaded files and classloader used to load their classes per version string.
     */
    private static final ConcurrentMap<String, HazelcastVersionClassloaderFuture> LOADED_VERSIONS =
            new ConcurrentHashMap<String, HazelcastVersionClassloaderFuture>();

    private static final File WORKING_DIRECTORY = Files.createTempDir();

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
        return newHazelcastInstance(version, configTemplate, enterprise, Collections.emptyList());
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
     * @param additionalJars
     * @return a {@link HazelcastInstance} proxying the started Hazelcast instance.
     */
    public static HazelcastInstance newHazelcastInstance(String version, Config configTemplate, boolean enterprise,
                                                         List<URL> additionalJars) {
        ClassLoader cfgClassLoader = configTemplate == null ? null : configTemplate.getClassLoader();
        HazelcastAPIDelegatingClassloader versionClassLoader = getTargetVersionClassloader(version, enterprise,
                cfgClassLoader, additionalJars);
        if (configTemplate != null) {
            configTemplate.getUserContext().put("versionClassLoader", versionClassLoader);
        }

        ClassLoader contextClassLoader = currentThread().getContextClassLoader();
        currentThread().setContextClassLoader(null);
        try {
            return newHazelcastInstanceWithNetwork(versionClassLoader, configTemplate);
        } finally {
            if (contextClassLoader != null) {
                currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    public static HazelcastInstance newHazelcastInstance(Config configTemplate, HazelcastAPIDelegatingClassloader classLoader) {
        if (configTemplate != null) {
            configTemplate.getUserContext().put("versionClassLoader", classLoader);
        }

        ClassLoader contextClassLoader = currentThread().getContextClassLoader();
        currentThread().setContextClassLoader(null);
        try {
            return newHazelcastInstanceWithNetwork(classLoader, configTemplate);
        } finally {
            if (contextClassLoader != null) {
                currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    public static Node getNode(HazelcastInstance hz) {
        try {
            HazelcastAPIDelegatingClassloader classloader = getHazelcastAPIDelegatingClassloader(hz);
            Object instance = getHazelcastInstanceImpl(hz, classloader);
            Object node = getFieldValueReflectively(instance, "node");
            return mock(Node.class, new NodeAnswer(node));
        } catch (HazelcastInstanceNotActiveException e) {
            throw new IllegalArgumentException("The given HazelcastInstance is not an active HazelcastInstanceImpl: "
                    + hz.getClass());
        } catch (Exception e) {
            throw rethrowGuardianException(e);
        }
    }

    public static HazelcastInstanceImpl getHazelcastInstanceImpl(HazelcastInstance hz) {
        try {
            HazelcastAPIDelegatingClassloader classloader = getHazelcastAPIDelegatingClassloader(hz);
            Object instance = getHazelcastInstanceImpl(hz, classloader);
            Object node = getFieldValueReflectively(instance, "node");
            HazelcastInstanceImpl proxy = mock(HazelcastInstanceImpl.class, new HazelcastInstanceImplAnswer(instance));
            setFieldValueReflectively(proxy, "node", mock(Node.class, new NodeAnswer(node)));
            return proxy;
        } catch (HazelcastInstanceNotActiveException e) {
            throw new IllegalArgumentException("The given HazelcastInstance is not an active HazelcastInstanceImpl: "
                    + hz.getClass());
        } catch (Exception e) {
            throw rethrowGuardianException(e);
        }
    }

    private static HazelcastAPIDelegatingClassloader getHazelcastAPIDelegatingClassloader(HazelcastInstance hz) {
        ConcurrentMap<String, Object> userContext = hz.getConfig().getUserContext();
        HazelcastAPIDelegatingClassloader classloader
                = (HazelcastAPIDelegatingClassloader) userContext.get("versionClassLoader");
        if (classloader == null) {
            throw new GuardianException("HazelcastInstance was not started via HazelcastStarter or without config template!");
        }
        return classloader;
    }

    private static Object getHazelcastInstanceImpl(HazelcastInstance hazelcastInstance, ClassLoader classloader)
            throws Exception {
        if (isProxyClass(hazelcastInstance.getClass())) {
            InvocationHandler invocationHandler = Proxy.getInvocationHandler(hazelcastInstance);
            Object delegate = getFieldValueReflectively(invocationHandler, "delegate");
            Class<?> instanceProxyClass = classloader.loadClass("com.hazelcast.instance.impl.HazelcastInstanceProxy");
            Class<?> instanceImplClass = classloader.loadClass("com.hazelcast.instance.impl.HazelcastInstanceImpl");
            if (instanceProxyClass.isAssignableFrom(delegate.getClass())) {
                Object instanceProxy = instanceProxyClass.cast(delegate);
                return instanceImplClass.cast(getFieldValueReflectively(instanceProxy, "original"));
            } else if (instanceImplClass.isAssignableFrom(delegate.getClass())) {
                return instanceImplClass.cast(delegate);
            }
            return delegate;
        }
        return TestUtil.getHazelcastInstanceImpl(hazelcastInstance);
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
        return getTargetVersionClassloader(version, enterprise, configClassLoader, Collections.emptyList());
    }

    /**
     * Obtains a {@link HazelcastAPIDelegatingClassloader} with the given version's binaries in its classpath.
     * Classloaders are cached, so requesting the classloader for a given version multiple times will return the
     * same instance.
     *
     * @param version           the target Hazelcast version e.g. "3.8.1", must be a published release version
     * @param configClassLoader class loader given via config
     * @param additionalJars
     * @return a classloader with given version's artifacts in its classpath
     */
    public static HazelcastAPIDelegatingClassloader getTargetVersionClassloader(String version, boolean enterprise,
                                                                                ClassLoader configClassLoader,
                                                                                List<URL> additionalJars) {
        HazelcastVersionClassloaderFuture future;
        if (configClassLoader != null) {
            // when a custom ClassLoader should be the parent of the target version ClassLoader,
            // do not use the ClassLoader cache
            future = new HazelcastVersionClassloaderFuture(version, enterprise, configClassLoader, additionalJars);
            return future.get();
        }

        String versionSpec = versionSpec(version, enterprise);
        future = LOADED_VERSIONS.get(versionSpec);
        if (future != null) {
            return future.get();
        }

        future = new HazelcastVersionClassloaderFuture(version, enterprise, null, additionalJars);
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

    private static HazelcastInstance newHazelcastInstanceWithNetwork(HazelcastAPIDelegatingClassloader classloader,
                                                                     Config configTemplate) {
        try {
            Object delegate = createInstanceViaInstanceFactory(classloader, configTemplate);
            if (delegate == null) {
                delegate = createInstanceViaHazelcast(classloader, configTemplate);
            }
            return (HazelcastInstance) proxyObjectForStarter(HazelcastStarter.class.getClassLoader(), delegate);
        } catch (ClassNotFoundException e) {
            throw rethrowGuardianException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Object createInstanceViaInstanceFactory(HazelcastAPIDelegatingClassloader classloader, Config configTemplate) {
        try {
            Class<?> configClass = classloader.loadClass("com.hazelcast.config.Config");
            Object config = getConfig(classloader, configClass, configTemplate);
            String instanceName = configTemplate == null ? newUnsecureUuidString() : configTemplate.getInstanceName();

            Class<?> nodeContextClass = classloader.loadClass("com.hazelcast.instance.impl.NodeContext");
            Class<?> firewallingNodeContextClass = classloader.loadClass("com.hazelcast.instance.FirewallingNodeContext");
            Object nodeContext = proxyObjectForStarter(classloader, firewallingNodeContextClass.newInstance());

            Class<?> hazelcastInstanceFactoryClass = classloader.loadClass("com.hazelcast.instance.impl.HazelcastInstanceFactory");
            System.out.println(hazelcastInstanceFactoryClass + " loaded by " + hazelcastInstanceFactoryClass.getClassLoader());

            Method newHazelcastInstanceMethod = hazelcastInstanceFactoryClass.getMethod("newHazelcastInstance", configClass,
                    String.class, nodeContextClass);
            return newHazelcastInstanceMethod.invoke(null, config, instanceName, nodeContext);
        } catch (ClassNotFoundException e) {
            debug("Could not create HazelcastInstance via HazelcastInstanceFactory: " + e.getMessage());
        } catch (IllegalAccessException e) {
            debug("Could not create HazelcastInstance via HazelcastInstanceFactory: " + e.getMessage());
        } catch (NoSuchMethodException e) {
            debug("Could not create HazelcastInstance via HazelcastInstanceFactory: " + e.getMessage());
        } catch (InvocationTargetException e) {
            debug("Could not create HazelcastInstance via HazelcastInstanceFactory: " + e.getMessage());
        } catch (InstantiationException e) {
            debug("Could not create HazelcastInstance via HazelcastInstanceFactory: " + e.getMessage());
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Object createInstanceViaHazelcast(HazelcastAPIDelegatingClassloader classloader, Config configTemplate) {
        try {
            Class<?> configClass = classloader.loadClass("com.hazelcast.config.Config");
            Object config = getConfig(classloader, configClass, configTemplate);

            Class<Hazelcast> hazelcastClass = (Class<Hazelcast>) classloader.loadClass("com.hazelcast.core.Hazelcast");
            System.out.println(hazelcastClass + " loaded by " + hazelcastClass.getClassLoader());

            Method newHazelcastInstanceMethod = hazelcastClass.getMethod("newHazelcastInstance", configClass);
            return newHazelcastInstanceMethod.invoke(null, config);
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
        }
    }

    public static Object getConfig(HazelcastAPIDelegatingClassloader classloader, Class<?> configClass, Object configTemplate)
            throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException,
            ClassNotFoundException {
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

    private static String versionSpec(String version, boolean enterprise) {
        return enterprise ? version + "-EE" : version;
    }

    private static class HazelcastVersionClassloaderFuture {

        private final String version;
        private final boolean enterprise;
        private final ClassLoader configClassLoader;
        private final List<URL> additionalJars = new ArrayList<>();

        private HazelcastAPIDelegatingClassloader classLoader;

        HazelcastVersionClassloaderFuture(String version, boolean enterprise, ClassLoader configClassLoader,
                                          List<URL> additionalJars) {
            this.version = version;
            this.enterprise = enterprise;
            this.configClassLoader = configClassLoader;
            this.additionalJars.addAll(additionalJars);
        }

        public HazelcastAPIDelegatingClassloader get() {
            if (classLoader != null) {
                return classLoader;
            }
            synchronized (this) {
                File versionDir = getOrCreateVersionDirectory(versionSpec(version, enterprise));
                File[] files = HazelcastVersionLocator.locateVersion(version, versionDir, enterprise);
                List<URL> urls = fileIntoUrls(files);
                urls.addAll(additionalJars);
                ClassLoader parentClassloader = HazelcastStarter.class.getClassLoader();
                if (configClassLoader != null) {
                    parentClassloader = configClassLoader;
                }
                classLoader = new HazelcastAPIDelegatingClassloader(urls.toArray(new URL[]{}), parentClassloader);
                return classLoader;
            }
        }

        private static List<URL> fileIntoUrls(File[] files) {
            List<URL> urls = new ArrayList<>(files.length);
            for (File file : files) {
                try {
                    urls.add(file.toURI().toURL());
                } catch (MalformedURLException e) {
                    throw rethrowGuardianException(e);
                }
            }
            return urls;
        }
    }
}
