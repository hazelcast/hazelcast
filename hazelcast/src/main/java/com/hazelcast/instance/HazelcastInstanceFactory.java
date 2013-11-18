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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.util.ValidationUtil.hasText;

@SuppressWarnings("SynchronizationOnStaticField")
@PrivateApi
public final class HazelcastInstanceFactory {

    private static final ConcurrentMap<String, InstanceFuture> instanceMap
            = new ConcurrentHashMap<String, InstanceFuture>(5);

    private static final AtomicInteger factoryIdGen = new AtomicInteger();

    public static Set<HazelcastInstance> getAllHazelcastInstances() {
        Set<HazelcastInstance> result = new HashSet<HazelcastInstance>();
        for (InstanceFuture f : instanceMap.values()) {
            result.add(f.get());
        }

        return result;
    }

    public static HazelcastInstance getHazelcastInstance(String instanceName) {
        InstanceFuture instanceFuture = instanceMap.get(instanceName);
        if (instanceFuture == null) {
            return null;
        }

        return instanceFuture.get();
    }

    public static HazelcastInstance getOrCreateHazelcastInstance(Config config) {
        if (config == null) {
            throw new NullPointerException("config can't be null");
        }

        String name = config.getInstanceName();
        hasText(name, "instanceName");

        InstanceFuture future = instanceMap.get(name);
        if (future != null) {
            return future.get();
        }

        future = new InstanceFuture();
        InstanceFuture found = instanceMap.putIfAbsent(name, future);
        if (found != null) {
            return found.get();
        }

        HazelcastInstanceProxy hz = constructHazelcastInstance(config, name, new DefaultNodeContext());
        future.set(hz);
        return hz;
    }

    public static HazelcastInstance newHazelcastInstance(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }

        HazelcastInstanceProxy hz = newHazelcastInstance(config, config.getInstanceName(), new DefaultNodeContext());
        return hz;
    }

    private static String createInstanceName(Config config) {
        return "_hzInstance_" + factoryIdGen.incrementAndGet() + "_" + config.getGroupConfig().getName();
    }

    public static HazelcastInstanceProxy newHazelcastInstance(Config config, String instanceName, NodeContext nodeContext) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }

        String name = instanceName;
        if (name == null || name.trim().length() == 0) {
            name = createInstanceName(config);
        }

        InstanceFuture future = new InstanceFuture();
        if (instanceMap.putIfAbsent(name, future) != null) {
            throw new DuplicateInstanceNameException("HazelcastInstance with name '" + name + "' already exists!");
        }

        HazelcastInstanceProxy hz = constructHazelcastInstance(config, name, nodeContext);
        future.set(hz);
        return hz;
    }

    private static HazelcastInstanceProxy constructHazelcastInstance(Config config, String instanceName, NodeContext nodeContext) {
        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();

        HazelcastInstanceProxy proxy;
        try {
            Thread.currentThread().setContextClassLoader(HazelcastInstanceFactory.class.getClassLoader());

            final HazelcastInstanceImpl hazelcastInstance = new HazelcastInstanceImpl(instanceName, config, nodeContext);
            OutOfMemoryErrorDispatcher.register(hazelcastInstance);
            proxy = new HazelcastInstanceProxy(hazelcastInstance);
            final Node node = hazelcastInstance.node;
            final Iterator<Member> iter = node.getClusterService().getMembers().iterator();
            final boolean firstMember = (iter.hasNext() && iter.next().localMember());
            final int initialWaitSeconds = node.groupProperties.INITIAL_WAIT_SECONDS.getInteger();
            if (initialWaitSeconds > 0) {
                hazelcastInstance.logger.info("Waiting " + initialWaitSeconds + " seconds before completing HazelcastInstance startup...");
                try {
                    Thread.sleep(initialWaitSeconds * 1000);
                    if (firstMember) {
                        node.partitionService.firstArrangement();
                    } else {
                        Thread.sleep(4 * 1000);
                    }
                } catch (InterruptedException ignored) {
                }
            }
            final int initialMinClusterSize = node.groupProperties.INITIAL_MIN_CLUSTER_SIZE.getInteger();
            while (node.getClusterService().getSize() < initialMinClusterSize) {
                try {
                    hazelcastInstance.logger.info("HazelcastInstance waiting for cluster size of " + initialMinClusterSize);
                    //noinspection BusyWait
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
            if (initialMinClusterSize > 1) {
                if (firstMember) {
                    node.partitionService.firstArrangement();
                } else {
                    Thread.sleep(3 * 1000);
                }
                hazelcastInstance.logger.info("HazelcastInstance starting after waiting for cluster size of "
                        + initialMinClusterSize);
            }
            hazelcastInstance.lifecycleService.fireLifecycleEvent(STARTED);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
        return proxy;
    }

    public static void shutdownAll() {
        final List<HazelcastInstanceProxy> instances = new LinkedList<HazelcastInstanceProxy>();
        for(InstanceFuture f: instanceMap.values()){
            instances.add(f.get());
        }

        instanceMap.clear();
        OutOfMemoryErrorDispatcher.clear();
        ManagementService.shutdownAll();
        Collections.sort(instances, new Comparator<HazelcastInstanceProxy>() {
            public int compare(HazelcastInstanceProxy o1, HazelcastInstanceProxy o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        for (HazelcastInstanceProxy proxy : instances) {
            proxy.getLifecycleService().shutdown();
            proxy.original = null;
        }
    }

    static void remove(HazelcastInstanceImpl instance) {
        OutOfMemoryErrorDispatcher.deregister(instance);

        InstanceFuture future = instanceMap.remove(instance.getName());
        if(future != null){
            future.get().original = null;
        }

        if (instanceMap.size() == 0) {
            ManagementService.shutdownAll();
        }
    }

    private static class InstanceFuture {
        private volatile HazelcastInstanceProxy hz;

        HazelcastInstanceProxy get() {
            if (hz != null) {
                return hz;
            }

            boolean restoreInterrupt = false;
            synchronized (this) {
                while (hz == null) {
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

            return hz;
        }

        void set(HazelcastInstanceProxy proxy) {
            this.hz = proxy;
            synchronized (this) {
                notifyAll();
            }
        }
    }
}
