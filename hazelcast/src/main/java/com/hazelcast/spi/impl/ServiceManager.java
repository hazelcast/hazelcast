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

package com.hazelcast.spi.impl;

import com.hazelcast.atomicnumber.AtomicNumberService;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.config.CustomServiceConfig;
import com.hazelcast.config.MapServiceConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.Services;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.countdownlatch.CountDownLatchService;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.ClientProtocolService;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.topic.TopicService;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * @mdogan 9/18/12
 */

@PrivateApi
class ServiceManager {

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final ConcurrentMap<String, Object> services = new ConcurrentHashMap<String, Object>(10, .75f, 1);

    ServiceManager(final NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ServiceManager.class.getName());
    }

    synchronized void start() {
        final Node node = nodeEngine.getNode();
        // register core services
        logger.log(Level.FINEST, "Registering core services...");
        registerService(ClusterService.SERVICE_NAME, node.getClusterService());
        registerService(PartitionService.SERVICE_NAME, node.getPartitionService());
        registerService(ProxyServiceImpl.NAME, nodeEngine.getProxyService());

        final Services servicesConfig = node.getConfig().getServicesConfig();
        if (servicesConfig != null) {
            if (servicesConfig.isEnableDefaults()) {
                logger.log(Level.FINEST, "Registering default services...");
                registerService(MapService.MAP_SERVICE_NAME, new MapService(nodeEngine));
                registerService(QueueService.QUEUE_SERVICE_NAME, new QueueService(nodeEngine));
                registerService(AtomicNumberService.NAME, new AtomicNumberService());
                registerService(TopicService.NAME, new TopicService());
                registerService(CollectionService.COLLECTION_SERVICE_NAME, new CollectionService(nodeEngine));
                registerService(CountDownLatchService.SERVICE_NAME, new CountDownLatchService());
                registerService(DistributedExecutorService.SERVICE_NAME, new DistributedExecutorService());
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
                                logger.log(Level.SEVERE, e.getMessage(), e);
                            }
                        }
                    } else if (serviceConfig instanceof MapServiceConfig) {
                        if (!services.containsKey(MapServiceConfig.SERVICE_NAME)) {
                            service = new MapService(nodeEngine);
                        }
                    }
                    if (service != null) {
                        registerService(serviceConfig.getName(), service);
                    }
                }
            }
        }
    }

    synchronized void shutdown() {
        logger.log(Level.FINEST, "Stopping services...");
        final List<ManagedService> managedServices = getServices(ManagedService.class);
        // reverse order to stop CoreServices last.
        Collections.reverse(managedServices);
        services.clear();
        for (ManagedService service : managedServices) {
            destroyService(service);
        }
    }

    private void destroyService(final ManagedService service) {
        try {
            logger.log(Level.FINEST, "Destroying service -> " + service);
            service.destroy();
        } catch (Throwable t) {
            logger.log(Level.SEVERE, "Error while stopping service: " + t.getMessage(), t);
        }
    }

    private synchronized void registerService(String serviceName, Object service) {
        logger.log(Level.FINEST, "Registering service: '" + serviceName + "'");
        Object oldService = services.putIfAbsent(serviceName, service);
        if (oldService != null) {
            logger.log(Level.WARNING, "Replacing " + serviceName + ": " +
                    oldService + " with " + service);
            if (oldService instanceof CoreService) {
                throw new HazelcastException("Can not replace a CoreService! Name: " + serviceName
                        + ", Service: " + oldService);
            }
            if (oldService instanceof ManagedService) {
                destroyService((ManagedService) oldService);
            }
            services.put(serviceName, service);
        }
        if (service instanceof ManagedService) {
            try {
                logger.log(Level.FINEST, "Initializing service -> " + serviceName + ": " + service);
                ((ManagedService) service).init(nodeEngine, new Properties());
            } catch (Throwable t) {
                logger.log(Level.SEVERE, "Error while initializing service: " + t.getMessage(), t);
            }
        }
        if (service instanceof ClientProtocolService) {
            nodeEngine.getNode().clientCommandService.register((ClientProtocolService) service);
        }
    }

    <T> T getService(String serviceName) {
        return (T) services.get(serviceName);
    }

    /**
     * Returns a list of services matching provided service class/interface.
     * <br></br>
     * <b>CoreServices will be placed at the beginning of the list.</b>
     */
    <S> List<S> getServices(Class<S> serviceClass) {
        final LinkedList<S> result = new LinkedList<S>();
        for (Object service : services.values()) {
            if (serviceClass.isAssignableFrom(service.getClass())) {
                if (service instanceof CoreService) {
                    result.addFirst((S) service);
                } else {
                    result.addLast((S) service);
                }
            }
        }
        return result;
    }
}
