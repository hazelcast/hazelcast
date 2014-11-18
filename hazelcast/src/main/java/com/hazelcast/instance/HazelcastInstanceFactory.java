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

package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.jmx.ManagementService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.util.ValidationUtil.hasText;

@SuppressWarnings("SynchronizationOnStaticField")
@PrivateApi
public final class HazelcastInstanceFactory {

    private static final ConcurrentMap<String, InstanceFuture> INSTANCE_MAP
            = new ConcurrentHashMap<String, InstanceFuture>(5);

    private static final AtomicInteger FACTORY_ID_GEN = new AtomicInteger();

    private HazelcastInstanceFactory() {
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
        if (config == null) {
            throw new NullPointerException("config can't be null");
        }

        String name = config.getInstanceName();
        hasText(name, "instanceName");

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

    public static HazelcastInstance newHazelcastInstance(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }

        return newHazelcastInstance(config, config.getInstanceName(), new DefaultNodeContext());
    }

    private static String createInstanceName(Config config) {
        return "_hzInstance_" + FACTORY_ID_GEN.incrementAndGet() + "_" + config.getGroupConfig().getName();
    }

    public static HazelcastInstance newHazelcastInstance(Config config, String instanceName,
                                                              NodeContext nodeContext) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }

        String name = instanceName;
        if (name == null || name.trim().length() == 0) {
            name = createInstanceName(config);
        }

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

    private static HazelcastInstanceProxy constructHazelcastInstance(Config config, String instanceName,
                                                                     NodeContext nodeContext, InstanceFuture future) {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        HazelcastInstanceProxy proxy;
        try {
            if (classLoader == null) {
                Thread.currentThread().setContextClassLoader(HazelcastInstanceFactory.class.getClassLoader());
            }
            HazelcastInstanceImpl hazelcastInstance = new HazelcastInstanceImpl(instanceName, config, nodeContext);
            OutOfMemoryErrorDispatcher.registerServer(hazelcastInstance);
            proxy = new HazelcastInstanceProxy(hazelcastInstance);
            final Node node = hazelcastInstance.node;
            final boolean firstMember = isFirstMember(node);
            final int initialWaitSeconds = node.groupProperties.INITIAL_WAIT_SECONDS.getInteger();
            if (initialWaitSeconds > 0) {
                hazelcastInstance.logger.info("Waiting "
                        + initialWaitSeconds + " seconds before completing HazelcastInstance startup...");
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(initialWaitSeconds));
                    if (firstMember) {
                        node.partitionService.firstArrangement();
                    } else {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(4));
                    }
                } catch (InterruptedException ignored) {
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
        final Iterator<Member> iter = node.getClusterService().getMembers().iterator();
        return (iter.hasNext() && iter.next().localMember());
    }

    private static void awaitMinimalClusterSize(HazelcastInstanceImpl hazelcastInstance, Node node, boolean firstMember)
            throws InterruptedException {

        final int initialMinClusterSize = node.groupProperties.INITIAL_MIN_CLUSTER_SIZE.getInteger();
        while (node.getClusterService().getSize() < initialMinClusterSize) {
            try {
                hazelcastInstance.logger.info("HazelcastInstance waiting for cluster size of " + initialMinClusterSize);
                //noinspection BusyWait
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException ignored) {
            }
        }
        if (initialMinClusterSize > 1) {
            if (firstMember) {
                node.partitionService.firstArrangement();
            } else {
                Thread.sleep(TimeUnit.SECONDS.toMillis(3));
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
        final List<HazelcastInstanceProxy> instances = new LinkedList<HazelcastInstanceProxy>();
        for (InstanceFuture f : INSTANCE_MAP.values()) {
            try {
                HazelcastInstanceProxy instanceProxy = f.get();
                instances.add(instanceProxy);
            } catch (RuntimeException ignore) {
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

    static Map<MemberImpl, HazelcastInstanceImpl> getInstanceImplMap() {
        final Map<MemberImpl, HazelcastInstanceImpl> map = new HashMap<MemberImpl, HazelcastInstanceImpl>();
        for (InstanceFuture f : INSTANCE_MAP.values()) {
            try {
                HazelcastInstanceProxy instanceProxy = f.get();
                final HazelcastInstanceImpl impl = instanceProxy.original;
                if (impl != null) {
                    map.put(impl.node.getLocalMember(), impl);
                }
            } catch (RuntimeException ignore) {
            }
        }
        return map;
    }

    static void remove(HazelcastInstanceImpl instance) {
        OutOfMemoryErrorDispatcher.deregisterServer(instance);
        InstanceFuture future = INSTANCE_MAP.remove(instance.getName());
        if (future != null) {
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
                    } catch (InterruptedException ignore) {
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
    }
}
