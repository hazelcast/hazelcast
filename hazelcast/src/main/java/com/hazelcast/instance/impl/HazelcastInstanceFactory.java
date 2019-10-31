/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.internal.config.XmlConfigLocator;
import com.hazelcast.config.YamlConfigBuilder;
import com.hazelcast.internal.config.YamlConfigLocator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.internal.util.ModularJavaUtils;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.validateSuffixInSystemProperty;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Central manager for all Hazelcast members of the JVM.
 *
 * All creation functionality will be stored here and a particular instance of a member will delegate here.
 */
@SuppressWarnings("SynchronizationOnStaticField")
public final class HazelcastInstanceFactory {

    private static final int ADDITIONAL_SLEEP_SECONDS_FOR_NON_FIRST_MEMBERS = 4;

    private static final AtomicInteger FACTORY_ID_GEN = new AtomicInteger();
    private static final ConcurrentMap<String, InstanceFuture> INSTANCE_MAP = new ConcurrentHashMap<String, InstanceFuture>(5);

    static {
        ModularJavaUtils.checkJavaInternalAccess(Logger.getLogger(HazelcastInstanceFactory.class));
    }

    private HazelcastInstanceFactory() {
    }

    public static Set<HazelcastInstance> getAllHazelcastInstances() {
        Set<HazelcastInstance> result = createHashSet(INSTANCE_MAP.size());
        for (InstanceFuture f : INSTANCE_MAP.values()) {
            result.add(f.get());
        }
        return result;
    }

    public static HazelcastInstance getHazelcastInstance(String instanceName) {
        InstanceFuture instanceFuture = INSTANCE_MAP.get(instanceName);
        if (instanceFuture == null) {
            return null;
        }

        try {
            return instanceFuture.get();
        } catch (IllegalStateException t) {
            return null;
        }
    }

    public static HazelcastInstance getOrCreateHazelcastInstance(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }

        String name = config.getInstanceName();
        checkHasText(name, "instanceName must contain text");

        InstanceFuture future = INSTANCE_MAP.get(name);
        if (future != null) {
            return future.get();
        }

        future = new InstanceFuture();
        InstanceFuture found = INSTANCE_MAP.putIfAbsent(name, future);
        if (found != null) {
            return found.get();
        }

        try {
            return constructHazelcastInstance(config, name, new DefaultNodeContext(), future);
        } catch (Throwable t) {
            INSTANCE_MAP.remove(name, future);
            future.setFailure(t);
            throw ExceptionUtil.rethrow(t);
        }
    }

    /**
     * Creates a new Hazelcast instance.
     *
     * @param config the configuration to use; if <code>null</code>, the set of defaults
     *               as specified in the XSD for the configuration XML will be used.
     * @return the configured {@link HazelcastInstance}
     */
    public static HazelcastInstance newHazelcastInstance(Config config) {
        if (config == null) {
            validateSuffixInSystemProperty(SYSPROP_MEMBER_CONFIG);

            XmlConfigLocator xmlConfigLocator = new XmlConfigLocator();
            YamlConfigLocator yamlConfigLocator = new YamlConfigLocator();

            if (xmlConfigLocator.locateFromSystemProperty()) {
                // 1. Try loading XML config from the configuration provided in system property
                config = new XmlConfigBuilder(xmlConfigLocator).build();

            } else if (yamlConfigLocator.locateFromSystemProperty()) {
                // 2. Try loading YAML config from the configuration provided in system property
                config = new YamlConfigBuilder(yamlConfigLocator).build();

            } else if (xmlConfigLocator.locateInWorkDirOrOnClasspath()) {
                // 3. Try loading XML config from the working directory or from the classpath
                config = new XmlConfigBuilder(xmlConfigLocator).build();

            } else if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
                // 4. Try loading YAML config from the working directory or from the classpath
                config = new YamlConfigBuilder(yamlConfigLocator).build();

            } else {
                // 5. Loading the default XML configuration file
                xmlConfigLocator.locateDefault();
                config = new XmlConfigBuilder(xmlConfigLocator).build();
            }
        }

        return newHazelcastInstance(
                config,
                config.getInstanceName(),
                new DefaultNodeContext()
        );
    }

    public static String createInstanceName(Config config) {
        String propertyValue = config.getProperty(GroupProperty.MOBY_NAMING_ENABLED.getName());
        if (propertyValue == null) {
            propertyValue = GroupProperty.MOBY_NAMING_ENABLED.getDefaultValue();
        }
        Boolean useMobyNaming = Boolean.valueOf(propertyValue);
        int instanceNum = FACTORY_ID_GEN.incrementAndGet();
        String name;
        if (useMobyNaming) {
            name = MobyNames.getRandomName(instanceNum);
        } else {
            name = "_hzInstance_" + instanceNum + "_" + config.getClusterName();
        }
        return name;
    }

    /**
     * Return real name for the hazelcast instance's instance
     *
     * @param instanceName -  template of the name
     * @param config       -   config
     * @return -   real hazelcast instance's name
     */
    public static String getInstanceName(String instanceName, Config config) {
        String name = instanceName;

        if (name == null || name.trim().length() == 0) {
            name = createInstanceName(config);
        }

        return name;
    }

    /**
     * Creates a new Hazelcast instance.
     *
     * @param config       the configuration to use; if <code>null</code>, the set of defaults
     *                     as specified in the XSD for the configuration XML will be used.
     * @param instanceName the name of the {@link HazelcastInstance}
     * @param nodeContext  the {@link NodeContext} to use
     * @return the configured {@link HazelcastInstance}
     */
    public static HazelcastInstance newHazelcastInstance(Config config, String instanceName, NodeContext nodeContext) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }

        String name = getInstanceName(instanceName, config);

        InstanceFuture future = new InstanceFuture();
        if (INSTANCE_MAP.putIfAbsent(name, future) != null) {
            throw new InvalidConfigurationException("HazelcastInstance with name '" + name + "' already exists!");
        }

        try {
            return constructHazelcastInstance(config, name, nodeContext, future);
        } catch (Throwable t) {
            INSTANCE_MAP.remove(name, future);
            future.setFailure(t);
            throw ExceptionUtil.rethrow(t);
        }
    }

    private static HazelcastInstanceProxy newHazelcastProxy(HazelcastInstanceImpl hazelcastInstance) {
        return new HazelcastInstanceProxy(hazelcastInstance);
    }

    private static HazelcastInstanceProxy constructHazelcastInstance(Config config, String instanceName, NodeContext nodeContext,
                                                                     InstanceFuture future) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        HazelcastInstanceProxy proxy;
        try {
            if (classLoader == null) {
                Thread.currentThread().setContextClassLoader(HazelcastInstanceFactory.class.getClassLoader());
            }
            HazelcastInstanceImpl hazelcastInstance = new HazelcastInstanceImpl(instanceName, config, nodeContext);
            OutOfMemoryErrorDispatcher.registerServer(hazelcastInstance);

            proxy = newHazelcastProxy(hazelcastInstance);

            Node node = hazelcastInstance.node;
            boolean firstMember = isFirstMember(node);
            long initialWaitSeconds = node.getProperties().getSeconds(GroupProperty.INITIAL_WAIT_SECONDS);
            if (initialWaitSeconds > 0) {
                hazelcastInstance.logger.info(format("Waiting %d seconds before completing HazelcastInstance startup...",
                        initialWaitSeconds));
                try {
                    SECONDS.sleep(initialWaitSeconds);
                    if (firstMember) {
                        node.partitionService.firstArrangement();
                    } else {
                        SECONDS.sleep(ADDITIONAL_SLEEP_SECONDS_FOR_NON_FIRST_MEMBERS);
                    }
                } catch (InterruptedException ignored) {
                    currentThread().interrupt();
                }
            }
            awaitMinimalClusterSize(hazelcastInstance, node, firstMember);
            future.set(proxy);
            hazelcastInstance.lifecycleService.fireLifecycleEvent(STARTED);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
        return proxy;
    }

    private static boolean isFirstMember(Node node) {
        Iterator<Member> iterator = node.getClusterService().getMembers().iterator();
        return (iterator.hasNext() && iterator.next().localMember());
    }

    private static void awaitMinimalClusterSize(HazelcastInstanceImpl hazelcastInstance, Node node, boolean firstMember)
            throws InterruptedException {

        int initialMinClusterSize = node.getProperties().getInteger(GroupProperty.INITIAL_MIN_CLUSTER_SIZE);
        while (node.getClusterService().getSize() < initialMinClusterSize) {
            try {
                hazelcastInstance.logger.info("HazelcastInstance waiting for cluster size of " + initialMinClusterSize);
                // noinspection BusyWait
                SECONDS.sleep(1);
            } catch (InterruptedException ignored) {
                currentThread().interrupt();
            }
        }
        if (initialMinClusterSize > 1) {
            if (firstMember) {
                node.partitionService.firstArrangement();
            } else {
                SECONDS.sleep(3);
            }
            hazelcastInstance.logger.info("HazelcastInstance starting after waiting for cluster size of "
                    + initialMinClusterSize);
        }
    }

    public static void shutdownAll() {
        shutdownAll(false);
    }

    public static void terminateAll() {
        shutdownAll(true);
    }

    private static void shutdownAll(boolean terminate) {
        List<HazelcastInstanceProxy> instances = new LinkedList<HazelcastInstanceProxy>();
        for (InstanceFuture future : INSTANCE_MAP.values()) {
            try {
                HazelcastInstanceProxy instanceProxy = future.get();
                instances.add(instanceProxy);
            } catch (RuntimeException ignored) {
                ignore(ignored);
            }
        }

        INSTANCE_MAP.clear();
        OutOfMemoryErrorDispatcher.clearServers();
        ManagementService.shutdownAll(instances);
        Collections.sort(instances, new Comparator<HazelcastInstanceProxy>() {
            public int compare(HazelcastInstanceProxy o1, HazelcastInstanceProxy o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        for (HazelcastInstanceProxy proxy : instances) {
            if (terminate) {
                proxy.getLifecycleService().terminate();
            } else {
                proxy.getLifecycleService().shutdown();
            }
            proxy.original = null;
        }
    }

    public static void remove(HazelcastInstanceImpl instance) {
        OutOfMemoryErrorDispatcher.deregisterServer(instance);
        InstanceFuture future = INSTANCE_MAP.remove(instance.getName());
        if (future != null && future.isSet()) {
            future.get().original = null;
        }
        if (INSTANCE_MAP.size() == 0) {
            ManagementService.shutdown(instance.getName());
        }
    }

    private static class InstanceFuture {

        private volatile HazelcastInstanceProxy hz;
        private volatile Throwable throwable;

        HazelcastInstanceProxy get() {
            if (hz != null) {
                return hz;
            }

            boolean restoreInterrupt = false;
            synchronized (this) {
                while (hz == null && throwable == null) {
                    try {
                        wait();
                    } catch (InterruptedException ignored) {
                        restoreInterrupt = true;
                    }
                }
            }

            if (restoreInterrupt) {
                Thread.currentThread().interrupt();
            }

            if (hz != null) {
                return hz;
            }

            throw new IllegalStateException(throwable);
        }

        void set(HazelcastInstanceProxy proxy) {
            synchronized (this) {
                this.hz = proxy;
                notifyAll();
            }
        }

        public void setFailure(Throwable throwable) {
            synchronized (this) {
                this.throwable = throwable;
                notifyAll();
            }
        }

        boolean isSet() {
            return hz != null;
        }
    }
}
