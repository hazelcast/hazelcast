/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings("SynchronizationOnStaticField")
@PrivateApi
public final class HazelcastInstanceManager {
    /***
     * Instance for clientManagers
     */
    private static final HazelcastInstanceFactory INSTANCE_FACTORY;
    private static final AtomicInteger FACTORY_ID_GEN = new AtomicInteger();
    private static final int ADDITIONAL_SLEEP_SECONDS_FOR_NON_FIRST_MEMBERS = 4;
    private static final ConcurrentMap<String, InstanceFuture> INSTANCE_MAP = new ConcurrentHashMap<String, InstanceFuture>(5);

    static {
        ServiceLoader serviceLoader = ServiceLoader.load(HazelcastInstanceFactory.class);
        Iterator iterator = serviceLoader.iterator();
        if (iterator.hasNext()) {
            INSTANCE_FACTORY = ServiceLoader.load(HazelcastInstanceFactory.class).iterator().next();
        } else {
            INSTANCE_FACTORY = new DefaultHazelcastInstanceFactory();
        }
    }

    private HazelcastInstanceManager() {

    }

    public static Set<HazelcastInstance> getAllHazelcastInstances() {
        Set<HazelcastInstance> result = new HashSet<HazelcastInstance>();
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
        checkNotNull(config, "config can't be null");

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
            config = new XmlConfigBuilder().build();
        }

        return newHazelcastInstance(
                config,
                config.getInstanceName(),
                new DefaultNodeContext()
        );
    }

    private static String createInstanceName(Config config) {
        return "_hzInstance_" + FACTORY_ID_GEN.incrementAndGet() + "_" + config.getGroupConfig().getName();
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
            throw new DuplicateInstanceNameException("HazelcastInstance with name '" + name + "' already exists!");
        }

        try {
            return constructHazelcastInstance(config, name, nodeContext, future);
        } catch (Throwable t) {
            INSTANCE_MAP.remove(name, future);
            future.setFailure(t);
            throw ExceptionUtil.rethrow(t);
        }
    }

    public static Set<HazelcastInstanceImpl> getInstanceImpls(Collection<Member> members) {
        Set<HazelcastInstanceImpl> set = new HashSet<HazelcastInstanceImpl>();
        for (InstanceFuture future : INSTANCE_MAP.values()) {
            try {
                if (future.isSet()) {
                    HazelcastInstanceProxy instanceProxy = future.get();
                    HazelcastInstanceImpl impl = instanceProxy.original;
                    if (impl != null) {
                        final MemberImpl localMember = impl.node.getLocalMember();
                        if (members.contains(localMember)) {
                            set.add(impl);
                        }
                    }
                }
            } catch (RuntimeException ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
        return set;
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
            HazelcastInstanceImpl hazelcastInstance =
                    (HazelcastInstanceImpl) INSTANCE_FACTORY.newHazelcastInstance(config, instanceName, nodeContext);
            OutOfMemoryErrorDispatcher.registerServer(hazelcastInstance);

            proxy = newHazelcastProxy(hazelcastInstance);

            Node node = hazelcastInstance.node;
            boolean firstMember = isFirstMember(node);
            long initialWaitSeconds = node.getProperties().getSeconds(GroupProperty.INITIAL_WAIT_SECONDS);
            if (initialWaitSeconds > 0) {
                hazelcastInstance.logger.info(format("Waiting %d ms before completing HazelcastInstance startup...",
                        initialWaitSeconds));
                try {
                    SECONDS.sleep(initialWaitSeconds);
                    if (firstMember) {
                        node.partitionService.firstArrangement();
                    } else {
                        SECONDS.sleep(ADDITIONAL_SLEEP_SECONDS_FOR_NON_FIRST_MEMBERS);
                    }
                } catch (InterruptedException ignored) {
                    EmptyStatement.ignore(ignored);
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
                EmptyStatement.ignore(ignored);
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
                EmptyStatement.ignore(ignored);
            }
        }

        INSTANCE_MAP.clear();
        OutOfMemoryErrorDispatcher.clearServers();
        ManagementService.shutdownAll();
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

    public static Map<MemberImpl, HazelcastInstanceImpl> getInstanceImplMap() {
        Map<MemberImpl, HazelcastInstanceImpl> map = new HashMap<MemberImpl, HazelcastInstanceImpl>();
        for (InstanceFuture future : INSTANCE_MAP.values()) {
            try {
                HazelcastInstanceProxy instanceProxy = future.get();
                HazelcastInstanceImpl impl = instanceProxy.original;
                if (impl != null) {
                    map.put(impl.node.getLocalMember(), impl);
                }
            } catch (RuntimeException ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
        return map;
    }

    public static void remove(HazelcastInstanceImpl instance) {
        OutOfMemoryErrorDispatcher.deregisterServer(instance);
        InstanceFuture future = INSTANCE_MAP.remove(instance.getName());
        if (future != null && future.isSet()) {
            future.get().original = null;
        }
        if (INSTANCE_MAP.size() == 0) {
            ManagementService.shutdownAll();
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
