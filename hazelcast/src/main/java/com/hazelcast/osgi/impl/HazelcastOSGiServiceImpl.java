/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.osgi.HazelcastOSGiInstance;
import com.hazelcast.osgi.HazelcastOSGiService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.StringUtil;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

import java.util.Collections;
import java.util.Dictionary;
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

    private static final String DEFAULT_ID =
            BuildInfoProvider.getBuildInfo().getVersion()
            + "#"
            + (BuildInfoProvider.getBuildInfo().isEnterprise() ? "EE" : "OSS");

    private static final ServiceRegistration EMPTY_SERVICE_REGISTRATION
            = new ServiceRegistration() {
                @Override
                public ServiceReference getReference() {
                    return null;
                }

                @Override
                public void setProperties(Dictionary properties) {
                }

                @Override
                public void unregister() {
                }
            };

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

    public HazelcastOSGiServiceImpl(Bundle ownerBundle) {
        this(ownerBundle, DEFAULT_ID);
    }

    public HazelcastOSGiServiceImpl(Bundle ownerBundle, String id) {
        this.ownerBundle = ownerBundle;
        this.ownerBundleContext = ownerBundle.getBundleContext();
        this.id = id;
    }

    private void checkActive() {
        if (!isActive()) {
            throw new IllegalStateException("Hazelcast OSGI Service is not active!");
        }
    }

    private boolean shouldSetGroupName(GroupConfig groupConfig) {
        if (groupConfig == null
                || StringUtil.isNullOrEmpty(groupConfig.getName())
                || GroupConfig.DEFAULT_GROUP_NAME.equals(groupConfig.getName())) {
            if (!Boolean.getBoolean(HAZELCAST_OSGI_GROUPING_DISABLED)) {
                return true;
            }
        }
        return false;
    }

    private Config getConfig(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        GroupConfig groupConfig = config.getGroupConfig();
        if (shouldSetGroupName(groupConfig)) {
            String groupName = id;
            if (groupConfig == null) {
                config.setGroupConfig(new GroupConfig(groupName));
            } else {
                groupConfig.setName(groupName);
            }
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
        ServiceRegistration serviceRegistration;
        if (!Boolean.getBoolean(HAZELCAST_OSGI_REGISTER_DISABLED)) {
            serviceRegistration =
                    ownerBundleContext.registerService(HazelcastInstance.class.getName(), hazelcastOSGiInstance, null);
        } else {
            serviceRegistration = EMPTY_SERVICE_REGISTRATION;
        }
        instanceServiceRegistrationMap.put(hazelcastOSGiInstance, serviceRegistration);
        instanceMap.put(instance.getName(), hazelcastOSGiInstance);
        return hazelcastOSGiInstance;
    }

    private void deregisterInstance(HazelcastOSGiInstance hazelcastOSGiInstance) {
        instanceMap.remove(hazelcastOSGiInstance.getName());
        ServiceRegistration serviceRegistration =
                instanceServiceRegistrationMap.remove(hazelcastOSGiInstance);
        if (serviceRegistration != null && serviceRegistration != EMPTY_SERVICE_REGISTRATION) {
            ownerBundleContext.ungetService(serviceRegistration.getReference());
        }
    }

    private void shutdownAllInternal() {
        try {
            for (HazelcastOSGiInstance instance : instanceServiceRegistrationMap.keySet()) {
                try {
                    deregisterInstance(instance);
                } catch (Throwable t) {
                    LOGGER.finest("Error occurred while deregistering " + instance, t);
                }
                try {
                    instance.shutdown();
                } catch (Throwable t) {
                    LOGGER.finest("Error occurred while shutting down " + instance, t);
                }
            }
        } finally {
            if (hazelcastInstance != null) {
                // Default instance may not be registered due to set "HAZELCAST_OSGI_REGISTER_DISABLED" flag,
                // So be sure that is it shutdown.
                try {
                    hazelcastInstance.shutdown();
                } catch (Throwable t) {
                    LOGGER.finest("Error occurred while shutting down the default Hazelcast instance !", t);
                } finally {
                    hazelcastInstance = null;
                }
            }
        }
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
            if (!isActive()) {
                try {
                    if (hazelcastInstance != null) {
                        try {
                            LOGGER.warning("Default Hazelcast instance should be null while activating service !");
                            hazelcastInstance.shutdown();
                        } catch (Throwable t) {
                            LOGGER.finest("Error occurred while shutting down the default Hazelcast instance !", t);
                        } finally {
                            hazelcastInstance = null;
                        }
                    }
                    try {
                        if (Boolean.getBoolean(HAZELCAST_OSGI_START)) {
                            hazelcastInstance =
                                    new HazelcastOSGiInstanceImpl(createHazelcastInstance(null), this);
                            LOGGER.info("Default Hazelcast instance has been created");
                        }
                        if (hazelcastInstance != null && !Boolean.getBoolean(HAZELCAST_OSGI_REGISTER_DISABLED)) {
                            registerInstance(hazelcastInstance);
                            LOGGER.info("Default Hazelcast instance has been registered as OSGI service");
                        }
                    } catch (Throwable t) {
                        if (hazelcastInstance != null) {
                            shutdownHazelcastInstance(hazelcastInstance);
                            hazelcastInstance = null;
                        }
                        ExceptionUtil.rethrow(t);
                    }
                    serviceRegistration =
                            ownerBundleContext.registerService(HazelcastOSGiService.class.getName(), this, null);
                    LOGGER.info(this + " has been registered as OSGI service and activated now");
                } catch (Throwable t1) {
                    if (hazelcastInstance != null) {
                        // If somehow default instance is activated, revert and deactivate it.
                        try {
                            hazelcastInstance.shutdown();
                        }  catch (Throwable t2) {
                            LOGGER.finest("Error occurred while shutting down the default Hazelcast instance !", t2);
                        } finally {
                            hazelcastInstance = null;
                        }
                    }
                    ExceptionUtil.rethrow(t1);
                }
            }
        }
    }

    @Override
    public void deactivate() {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            if (isActive()) {
                try {
                    shutdownAllInternal();
                    try {
                        ownerBundleContext.ungetService(serviceRegistration.getReference());
                    } catch (Throwable t) {
                        LOGGER.finest("Error occurred while deregistering " + this, t);
                    }
                    LOGGER.info(this + " has been deregistered as OSGI service and deactivated");
                } catch (Throwable t) {
                    LOGGER.info(this + " will be forcefully deregistered as OSGI service "
                            + "and will be forcefully deactivated");
                    ExceptionUtil.rethrow(t);
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

        return Collections.unmodifiableSet(instanceServiceRegistrationMap.keySet());
    }

    @Override
    public void shutdownHazelcastInstance(HazelcastOSGiInstance instance) {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            checkActive();

            try {
                deregisterInstance(instance);
            } catch (Throwable t) {
                LOGGER.finest("Error occurred while deregistering " + instance, t);
            }
            try {
                instance.shutdown();
            } catch (Throwable t) {
                LOGGER.finest("Error occurred while shutting down " + instance, t);
            }
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
