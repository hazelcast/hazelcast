/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.CustomServiceConfig;
import com.hazelcast.config.MapServiceConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.Services;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * @mdogan 9/18/12
 */

@PrivateApi
class ServiceManager {

    private final NodeServiceImpl nodeService;
    private final ConcurrentMap<String, Object> services = new ConcurrentHashMap<String, Object>(10);

    ServiceManager(final NodeServiceImpl nodeService) {
        this.nodeService = nodeService;
    }

    void startServices() {
        final Node node = nodeService.getNode();
        // register core services
        registerService(ClusterService.SERVICE_NAME, node.getClusterService());
        registerService(PartitionService.SERVICE_NAME, node.getPartitionService());

        final Services servicesConfig = node.getConfig().getServicesConfig();
        if (servicesConfig != null) {
            if (servicesConfig.isEnableDefaults()) {
                registerService(MapService.MAP_SERVICE_NAME, new MapService(nodeService));
                // TODO: add other services
                // ...
                // ...
            }

            final Collection<ServiceConfig> serviceConfigs = servicesConfig.getServiceConfigs();
            for (ServiceConfig serviceConfig : serviceConfigs) {
                if (serviceConfig.isEnabled()) {
                    Object service = null;
                    if (serviceConfig instanceof CustomServiceConfig) {
                        final CustomServiceConfig customServiceConfig = (CustomServiceConfig) serviceConfig;
                        service = customServiceConfig.getServiceImpl();
                        if (service == null) {
                            try {
                                service = ClassLoaderUtil.newInstance(customServiceConfig.getClassName());
                            } catch (Throwable e) {
                                getLogger().log(Level.SEVERE, e.getMessage(), e);
                            }
                        }
                    } else if (serviceConfig instanceof MapServiceConfig) {
                        if (!services.containsKey(MapServiceConfig.SERVICE_NAME)) {
                            service = new MapService(nodeService);
                        }
                    }

                    if (service != null) {
                        registerService(serviceConfig.getName(), service);
                    }
                }
            }
        }
    }

    void stopServices() {
        final Collection<ManagedService> managedServices = getServices(ManagedService.class, false);
        services.clear();
        for (ManagedService service : managedServices) {
            try {
                service.destroy();
            } catch (Throwable t) {
                getLogger().log(Level.SEVERE, "Error while stopping service: " + t.getMessage(), t);
            }
        }
    }

    private ILogger getLogger() {
        return nodeService.getLogger(ServiceManager.class.getName());
    }

    private void registerService(String serviceName, Object service) {
        final ILogger logger = getLogger();
        logger.log(Level.FINEST, "Registering service: '" + serviceName + "'");
        Object oldService = services.put(serviceName, service);
        if (oldService != null) {
            logger.log(Level.WARNING, "Overriding '" + serviceName + "' using " + service);
            if (oldService instanceof CoreService) {
                services.put(serviceName, oldService);
                throw new HazelcastException("Can not replace a CoreService! Name: " + serviceName
                    + ", Service: " + oldService);
            }
        }
        if (service instanceof ManagedService) {
            try {
                ((ManagedService) service).init(nodeService, new Properties());
            } catch (Throwable t) {
                logger.log(Level.SEVERE, "Error while initializing service: " + t.getMessage(), t);
            }
        }
    }

    <T> T getService(String serviceName) {
        return (T) services.get(serviceName);
    }

    <S> Collection<S> getServices(Class<S> serviceClass, boolean coreServicesFirst) {
        final LinkedList<S> result = new LinkedList<S>();
        for (Object service : services.values()) {
            if (serviceClass.isAssignableFrom(service.getClass())) {
                if (service instanceof CoreService) {
                    if (coreServicesFirst) {
                        result.addFirst((S) service);
                    } else {
                        result.addLast((S) service);
                    }
                } else {
                    if (coreServicesFirst) {
                        result.addLast((S) service);
                    } else {
                        result.addFirst((S) service);
                    }
                }
            }
        }
        return result;
    }
}
