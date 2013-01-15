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
import com.hazelcast.jmx.ManagementService;
import com.hazelcast.util.Util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;

@SuppressWarnings("SynchronizationOnStaticField")
public class HazelcastInstanceFactory {

    private static final ConcurrentMap<String, HazelcastInstance> INSTANCE_MAP = new ConcurrentHashMap<String, HazelcastInstance>(5);

    private static final AtomicInteger factoryIdGen = new AtomicInteger();

    private static final Object INIT_LOCK = new Object();

    public static Set<HazelcastInstance> getAllHazelcastInstances() {
        return new HashSet<HazelcastInstance>(INSTANCE_MAP.values());
    }

    public static HazelcastInstance getHazelcastInstance(String instanceName) {
        return INSTANCE_MAP.get(instanceName);
    }

    public static HazelcastInstance newHazelcastInstance(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        return newHazelcastInstance(config, config.isLiteMember());
    }

    public static HazelcastInstance newHazelcastInstance(Config config, Boolean liteMember) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        if (liteMember != null) {
            config.setLiteMember(liteMember);
        }
        String name = config.getInstanceName();
        if (name == null || name.trim().length() == 0) {
            name = createInstanceName(config);
            return newHazelcastInstance(config, name);
        } else {
            synchronized (INIT_LOCK) {
                if (INSTANCE_MAP.containsKey(name)) {
                    throw new DuplicateInstanceNameException("HazelcastInstance with name '" + name + "' already exists!");
                }
                factoryIdGen.incrementAndGet();
                return newHazelcastInstance(config, name);
            }
        }
    }

    private static String createInstanceName(Config config) {
        return "_hzInstance_" + factoryIdGen.incrementAndGet() + "_" + config.getGroupConfig().getName();
    }

    static HazelcastInstance newHazelcastInstance(Config config, String instanceName) {
        return newHazelcastInstance(config, instanceName, new DefaultNodeContext());
    }

    static HazelcastInstance newHazelcastInstance(Config config, String instanceName, NodeContext nodeContext) {
        if (instanceName == null || instanceName.trim().length() == 0) {
            instanceName = createInstanceName(config);
        }
        HazelcastInstanceImpl hazelcastInstance = null;
        try {
            hazelcastInstance = new HazelcastInstanceImpl(instanceName, config, nodeContext);
            INSTANCE_MAP.put(instanceName, hazelcastInstance);
            final Node node = hazelcastInstance.node;
            boolean firstMember = (node.getClusterService().getMembers().iterator().next().localMember());
            int initialWaitSeconds = node.groupProperties.INITIAL_WAIT_SECONDS.getInteger();
            if (initialWaitSeconds > 0) {
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
            int initialMinClusterSize = node.groupProperties.INITIAL_MIN_CLUSTER_SIZE.getInteger();
            while (node.getClusterService().getMembers().size() < initialMinClusterSize) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
            if (initialMinClusterSize > 0) {
                if (firstMember) {
                    node.partitionService.firstArrangement();
                } else {
                    Thread.sleep(4 * 1000);
                }
                hazelcastInstance.logger.log(Level.INFO, "HazelcastInstance starting after waiting for cluster size of "
                        + initialMinClusterSize);
            }
            hazelcastInstance.lifecycleService.fireLifecycleEvent(STARTED);
            return hazelcastInstance;
        } catch (Throwable t) {
            if (hazelcastInstance != null) {
                hazelcastInstance.logger.log(Level.SEVERE, t.getMessage(), t);
            }
            Util.throwUncheckedException(t);
            return null;
        }
    }

    public static void shutdownAll() {
        Collection<HazelcastInstance> instances = INSTANCE_MAP.values();
        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().shutdown();
        }
        INSTANCE_MAP.clear();
        ManagementService.shutdown();
        ThreadContext.shutdownAll();
    }

    public static void remove(HazelcastInstance instance) {
        INSTANCE_MAP.remove(instance.getName());
        if (INSTANCE_MAP.size() == 0) {
            ManagementService.shutdown();
        }
    }
}
