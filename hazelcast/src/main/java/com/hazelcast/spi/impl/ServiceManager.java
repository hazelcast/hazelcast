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

import com.hazelcast.client.ClientEngineImpl;
import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.partition.PartitionServiceImpl;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.topic.TopicService;
import com.hazelcast.transaction.TransactionManagerServiceImpl;

import java.lang.reflect.Constructor;
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
        registerService(ClusterServiceImpl.SERVICE_NAME, node.getClusterService());
        registerService(PartitionServiceImpl.SERVICE_NAME, node.getPartitionService());
        registerService(ProxyServiceImpl.SERVICE_NAME, nodeEngine.getProxyService());
        registerService(TransactionManagerServiceImpl.SERVICE_NAME, nodeEngine.getTransactionManagerService());
        registerService(ClientEngineImpl.SERVICE_NAME, node.clientEngine);

        final ServicesConfig servicesConfigConfig = node.getConfig().getServicesConfig();
        final Map<String, Properties> serviceProps;
        if (servicesConfigConfig != null) {
            if (servicesConfigConfig.isEnableDefaults()) {
                logger.log(Level.FINEST, "Registering default services...");
                registerService(MapService.SERVICE_NAME, new MapService(nodeEngine));
                registerService(LockService.SERVICE_NAME, new LockService(nodeEngine));
                registerService(QueueService.SERVICE_NAME, new QueueService(nodeEngine));
                registerService(TopicService.SERVICE_NAME, new TopicService());
                registerService(CollectionService.SERVICE_NAME, new CollectionService(nodeEngine));
                registerService(DistributedExecutorService.SERVICE_NAME, new DistributedExecutorService());
                registerService(AtomicLongService.SERVICE_NAME, new AtomicLongService());
                registerService(CountDownLatchService.SERVICE_NAME, new CountDownLatchService());
                registerService(SemaphoreService.SERVICE_NAME, new SemaphoreService(nodeEngine));
                registerService(IdGeneratorService.SERVICE_NAME, new IdGeneratorService(nodeEngine));
            }

            serviceProps = new HashMap<String, Properties>();
            final Collection<ServiceConfig> serviceConfigs = servicesConfigConfig.getServiceConfigs();
            for (ServiceConfig serviceConfig : serviceConfigs) {
                if (serviceConfig.isEnabled()) {
                    Object service = serviceConfig.getServiceImpl();
                    if (service == null) {
                        service = createServiceObject(serviceConfig.getClassName());
                    }
                    if (service != null) {
                        registerService(serviceConfig.getName(), service, serviceConfig.getProperties());
                    }
                }
            }
        } else {
            serviceProps = Collections.emptyMap();
        }

        for (Object service : services.values()) {
            if (service instanceof ManagedService) {
                try {
                    logger.log(Level.FINEST, "Initializing service -> " + service);
                    ((ManagedService) service).init(nodeEngine, serviceProps.get(((ManagedService) service).getServiceName()));
                } catch (Throwable t) {
                    logger.log(Level.SEVERE, "Error while initializing service: " + t.getMessage(), t);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Object createServiceObject(String className) {
        try {
            Class serviceClass = ClassLoaderUtil.loadClass(className);
            try {
                Constructor constructor = serviceClass.getConstructor(NodeEngine.class);
                return constructor.newInstance(nodeEngine);
            } catch (NoSuchMethodException ignored) {
            }
            return ClassLoaderUtil.newInstance(serviceClass);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    synchronized void shutdown() {
        logger.log(Level.FINEST, "Stopping services...");
        final List<ManagedService> managedServices = getServices(ManagedService.class);
        // reverse order to stop CoreServices last.
        Collections.reverse(managedServices);
        services.clear();
        for (ManagedService service : managedServices) {
            shutdownService(service);
        }
    }

    private void shutdownService(final ManagedService service) {
        try {
            logger.log(Level.FINEST, "Shutting down service -> " + service);
            service.shutdown();
        } catch (Throwable t) {
            logger.log(Level.SEVERE, "Error while shutting down service[" + service + "]: " + t.getMessage(), t);
        }
    }

    private synchronized void registerService(String serviceName, Object service) {
        registerService(serviceName, service, new Properties());
    }

    private synchronized void registerService(String serviceName, Object service, Properties props) {
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
                shutdownService((ManagedService) oldService);
            }
            services.put(serviceName, service);
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
