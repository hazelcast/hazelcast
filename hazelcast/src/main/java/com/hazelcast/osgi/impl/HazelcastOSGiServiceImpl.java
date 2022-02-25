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

package com.hazelcast.osgi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.osgi.HazelcastOSGiInstance;
import com.hazelcast.osgi.HazelcastOSGiService;
import com.hazelcast.internal.util.ExceptionUtil;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementation of {@link HazelcastInternalOSGiService}.
 *
 * @see HazelcastInternalOSGiService
 * @see com.hazelcast.osgi.HazelcastOSGiService
 */
class HazelcastOSGiServiceImpl
        implements HazelcastInternalOSGiService {

    private static final ILogger LOGGER = Logger.getLogger(HazelcastOSGiService.class);

    private final Object serviceMutex = new Object();

    private final Bundle ownerBundle;
    private final BundleContext ownerBundleContext;
    private final String id;
    private final ConcurrentMap<HazelcastOSGiInstance, ServiceRegistration> instanceServiceRegistrationMap
            = new ConcurrentHashMap<HazelcastOSGiInstance, ServiceRegistration>();
    private final ConcurrentMap<String, HazelcastOSGiInstance> instanceMap
            = new ConcurrentHashMap<String, HazelcastOSGiInstance>();

    // No need to "volatile" since this field is guarded and happens-before is handled by
    // `synchronized` blocks of `serviceMutex` instance.
    private ServiceRegistration serviceRegistration;

    // Defined as `volatile` because even though this field is written inside `synchronized` blocks,
    // it can be read from any thread without `synchronized` blocks and without `volatile` they may seen invalid value.
    private volatile HazelcastOSGiInstance hazelcastInstance;

    HazelcastOSGiServiceImpl(Bundle ownerBundle) {
        this(ownerBundle, DEFAULT_ID);
    }

    HazelcastOSGiServiceImpl(Bundle ownerBundle, String id) {
        this.ownerBundle = ownerBundle;
        this.ownerBundleContext = ownerBundle.getBundleContext();
        this.id = id;
    }

    private void checkActive() {
        if (!isActive()) {
            throw new IllegalStateException("Hazelcast OSGI Service is not active!");
        }
    }

    private boolean shouldSetClusterName(String clusterName) {
        return (isNullOrEmpty(clusterName) || Config.DEFAULT_CLUSTER_NAME.equals(clusterName))
                && !Boolean.getBoolean(HAZELCAST_OSGI_GROUPING_DISABLED);
    }

    private Config getConfig(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        if (shouldSetClusterName(config.getClusterName())) {
            config.setClusterName(id);
        }
        return config;
    }

    private HazelcastInstance createHazelcastInstance(Config config) {
        return Hazelcast.newHazelcastInstance(getConfig(config));
    }

    private HazelcastOSGiInstance registerInstance(HazelcastInstance instance) {
        HazelcastOSGiInstance hazelcastOSGiInstance;
        if (instance instanceof HazelcastOSGiInstance) {
            hazelcastOSGiInstance = (HazelcastOSGiInstance) instance;
        } else {
            hazelcastOSGiInstance = new HazelcastOSGiInstanceImpl(instance, this);
        }
        if (!Boolean.getBoolean(HAZELCAST_OSGI_REGISTER_DISABLED)) {
            ServiceRegistration serviceRegistration =
                    ownerBundleContext.registerService(HazelcastInstance.class.getName(), hazelcastOSGiInstance, null);
            instanceServiceRegistrationMap.put(hazelcastOSGiInstance, serviceRegistration);
        }
        instanceMap.put(instance.getName(), hazelcastOSGiInstance);
        return hazelcastOSGiInstance;
    }

    private void deregisterInstance(HazelcastOSGiInstance hazelcastOSGiInstance) {
        instanceMap.remove(hazelcastOSGiInstance.getName());
        ServiceRegistration serviceRegistration =
                instanceServiceRegistrationMap.remove(hazelcastOSGiInstance);
        if (serviceRegistration != null) {
            ownerBundleContext.ungetService(serviceRegistration.getReference());
            serviceRegistration.unregister();
        }
    }

    private void shutdownDefaultHazelcastInstanceIfActive() {
        if (hazelcastInstance != null) {
            shutdownHazelcastInstanceInternalSafely(hazelcastInstance);
            hazelcastInstance = null;
        }
    }

    private void shutdownAllInternal() {
        for (HazelcastOSGiInstance instance : instanceMap.values()) {
            shutdownHazelcastInstanceInternalSafely(instance);
        }
        // Default instance may not be registered due to set `HAZELCAST_OSGI_REGISTER_DISABLED` flag,
        // So be sure that it is shutdown.
        shutdownDefaultHazelcastInstanceIfActive();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Bundle getOwnerBundle() {
        return ownerBundle;
    }

    @Override
    public boolean isActive() {
        return ownerBundle.getState() == Bundle.ACTIVE;
    }

    @Override
    public void activate() {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            if (ownerBundle.getState() == Bundle.STARTING) {
                try {
                    if (hazelcastInstance != null) {
                        LOGGER.warning("Default Hazelcast instance should be null while activating service!");
                        shutdownDefaultHazelcastInstanceIfActive();
                    }
                    if (Boolean.getBoolean(HAZELCAST_OSGI_START)) {
                        hazelcastInstance =
                                new HazelcastOSGiInstanceImpl(createHazelcastInstance(null), this);
                        LOGGER.info("Default Hazelcast instance has been created");
                    }
                    if (hazelcastInstance != null && !Boolean.getBoolean(HAZELCAST_OSGI_REGISTER_DISABLED)) {
                        registerInstance(hazelcastInstance);
                        LOGGER.info("Default Hazelcast instance has been registered as OSGI service");
                    }
                    serviceRegistration =
                            ownerBundleContext.registerService(HazelcastOSGiService.class.getName(), this, null);
                    LOGGER.info(this + " has been registered as OSGI service and activated now");
                } catch (Throwable t) {
                    // If somehow default instance is activated, revert and deactivate it.
                    shutdownDefaultHazelcastInstanceIfActive();
                    ExceptionUtil.rethrow(t);
                }
            }
        }
    }

    @Override
    public void deactivate() {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            if (ownerBundle.getState() == Bundle.STOPPING) {
                try {
                    shutdownAllInternal();
                    try {
                        ownerBundleContext.ungetService(serviceRegistration.getReference());
                        serviceRegistration.unregister();
                    } catch (Throwable t) {
                        LOGGER.finest("Error occurred while deregistering " + this, t);
                    }
                    LOGGER.info(this + " has been deregistered as OSGI service and deactivated");
                } finally {
                    serviceRegistration = null;
                }
            }
        }
    }

    @Override
    public HazelcastOSGiInstance getDefaultHazelcastInstance() {
        checkActive();

        // No need to synchronization since this is not a mutating operation on instances.
        // If at this time service is deactivated, this method just returns terminated instance.

        return hazelcastInstance;
    }

    @Override
    public HazelcastOSGiInstance newHazelcastInstance(Config config) {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            checkActive();

            return registerInstance(createHazelcastInstance(config));
        }
    }

    @Override
    public HazelcastOSGiInstance newHazelcastInstance() {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            checkActive();

            return registerInstance(createHazelcastInstance(null));
        }
    }

    @Override
    public HazelcastOSGiInstance getHazelcastInstanceByName(String instanceName) {
        checkActive();

        // No need to synchronization since this is not a mutating operation on instances.
        // If at this time service is deactivated, this method just returns terminated instance.

        return instanceMap.get(instanceName);
    }

    @Override
    public Set<HazelcastOSGiInstance> getAllHazelcastInstances() {
        checkActive();

        // No need to synchronization since this is not a mutating operation on instances.
        // If at this time service is deactivated, this method just returns terminated instances.

        return new HashSet<HazelcastOSGiInstance>(instanceMap.values());
    }

    @Override
    public void shutdownHazelcastInstance(HazelcastOSGiInstance instance) {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            checkActive();

            shutdownHazelcastInstanceInternal(instance);
        }
    }

    private void shutdownHazelcastInstanceInternal(HazelcastOSGiInstance instance) {
        try {
            deregisterInstance(instance);
        } catch (Throwable t) {
            LOGGER.finest("Error occurred while deregistering " + instance, t);
        }
        instance.shutdown();
    }

    private void shutdownHazelcastInstanceInternalSafely(HazelcastOSGiInstance instance) {
        try {
            shutdownHazelcastInstanceInternal(instance);
        } catch (Throwable t) {
            LOGGER.finest("Error occurred while shutting down " + instance, t);
        }
    }

    @Override
    public void shutdownAll() {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            checkActive();

            shutdownAllInternal();
        }
    }

    @Override
    public String toString() {
        return "HazelcastOSGiServiceImpl{"
                    + "ownerBundle=" + ownerBundle
                    + ", hazelcastInstance=" + hazelcastInstance
                    + ", active=" + isActive()
                    + ", id=" + id
                + '}';
    }

}
