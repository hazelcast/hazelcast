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
import com.hazelcast.collection.list.ListService;
import com.hazelcast.collection.set.SetService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockServiceImpl;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapService;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.queue.impl.QueueService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ServiceInfo;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.EmptyStatement.ignore;

@PrivateApi
final class ServiceManager {

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final ConcurrentMap<String, ServiceInfo> services = new ConcurrentHashMap<String, ServiceInfo>(20, .75f, 1);

    ServiceManager(final NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ServiceManager.class);
    }

    synchronized void start() {
        Map<String, Properties> serviceProps = new HashMap<String, Properties>();
        Map<String, Object> serviceConfigObjects = new HashMap<String, Object>();

        registerServices(serviceProps, serviceConfigObjects);
        initServices(serviceProps, serviceConfigObjects);
    }

    private void registerServices(Map<String, Properties> serviceProps, Map<String, Object> serviceConfigObjects) {
        registerCoreServices();

        Node node = nodeEngine.getNode();
        ServicesConfig servicesConfig = node.getConfig().getServicesConfig();
        if (servicesConfig != null) {
            registerDefaultServices(servicesConfig);
            registerUserServices(servicesConfig, serviceProps, serviceConfigObjects);
        }
    }

    private void registerCoreServices() {
        logger.finest("Registering core services...");

        Node node = nodeEngine.getNode();
        registerService(ClusterServiceImpl.SERVICE_NAME, node.getClusterService());
        registerService(InternalPartitionService.SERVICE_NAME, node.getPartitionService());
        registerService(ProxyServiceImpl.SERVICE_NAME, nodeEngine.getProxyService());
        registerService(TransactionManagerServiceImpl.SERVICE_NAME, nodeEngine.getTransactionManagerService());
        registerService(ClientEngineImpl.SERVICE_NAME, node.clientEngine);
    }

    private void registerDefaultServices(ServicesConfig servicesConfig) {
        if (!servicesConfig.isEnableDefaults()) {
            return;
        }

        logger.finest("Registering default services...");
        registerService(MapService.SERVICE_NAME, MapService.create(nodeEngine));
        registerService(LockService.SERVICE_NAME, new LockServiceImpl(nodeEngine));
        registerService(QueueService.SERVICE_NAME, new QueueService(nodeEngine));
        registerService(TopicService.SERVICE_NAME, new TopicService());
        registerService(MultiMapService.SERVICE_NAME, new MultiMapService(nodeEngine));
        registerService(ListService.SERVICE_NAME, new ListService(nodeEngine));
        registerService(SetService.SERVICE_NAME, new SetService(nodeEngine));
        registerService(DistributedExecutorService.SERVICE_NAME, new DistributedExecutorService());
        registerService(AtomicLongService.SERVICE_NAME, new AtomicLongService());
        registerService(AtomicReferenceService.SERVICE_NAME, new AtomicReferenceService());
        registerService(CountDownLatchService.SERVICE_NAME, new CountDownLatchService());
        registerService(SemaphoreService.SERVICE_NAME, new SemaphoreService(nodeEngine));
        registerService(IdGeneratorService.SERVICE_NAME, new IdGeneratorService(nodeEngine));
        registerService(MapReduceService.SERVICE_NAME, new MapReduceService(nodeEngine));
        registerService(ReplicatedMapService.SERVICE_NAME, new ReplicatedMapService(nodeEngine));
    }

    private void initServices(Map<String, Properties> serviceProps, Map<String, Object> serviceConfigObjects) {
        for (ServiceInfo serviceInfo : services.values()) {
            initService(serviceProps, serviceConfigObjects, serviceInfo);
        }
    }

    private void initService(Map<String, Properties> serviceProps, Map<String, Object> serviceConfigObjects,
                             ServiceInfo serviceInfo) {
        final Object service = serviceInfo.getService();
        if (serviceInfo.isConfigurableService()) {
            try {
                if (logger.isFinestEnabled()) {
                    logger.finest("Configuring service -> " + service);
                }
                final Object configObject = serviceConfigObjects.get(serviceInfo.getName());
                ((ConfigurableService) service).configure(configObject);
            } catch (Throwable t) {
                logger.severe("Error while configuring service: " + t.getMessage(), t);
            }
        }
        if (serviceInfo.isManagedService()) {
            try {
                if (logger.isFinestEnabled()) {
                    logger.finest("Initializing service -> " + service);
                }
                final Properties props = serviceProps.get(serviceInfo.getName());
                ((ManagedService) service).init(nodeEngine, props != null ? props : new Properties());
            } catch (Throwable t) {
                logger.severe("Error while initializing service: " + t.getMessage(), t);
            }
        }
    }

    private void registerUserServices(ServicesConfig servicesConfig, Map<String, Properties> serviceProps,
                                      Map<String, Object> serviceConfigObjects) {
        logger.finest("Registering user defined services...");
        Collection<ServiceConfig> serviceConfigs = servicesConfig.getServiceConfigs();
        for (ServiceConfig serviceConfig : serviceConfigs) {
            registerUserService(serviceProps, serviceConfigObjects, serviceConfig);
        }
    }

    private void registerUserService(Map<String, Properties> serviceProps, Map<String, Object> serviceConfigObjects,
                                     ServiceConfig serviceConfig) {
        if (!serviceConfig.isEnabled()) {
            return;
        }

        Object service = serviceConfig.getServiceImpl();
        if (service == null) {
            service = createServiceObject(serviceConfig.getClassName());
        }
        if (service != null) {
            registerService(serviceConfig.getName(), service);
            serviceProps.put(serviceConfig.getName(), serviceConfig.getProperties());
            if (serviceConfig.getConfigObject() != null) {
                serviceConfigObjects.put(serviceConfig.getName(), serviceConfig.getConfigObject());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Object createServiceObject(String className) {
        try {
            ClassLoader classLoader = nodeEngine.getConfigClassLoader();
            Class serviceClass = ClassLoaderUtil.loadClass(classLoader, className);
            try {
                Constructor constructor = serviceClass.getConstructor(NodeEngine.class);
                return constructor.newInstance(nodeEngine);
            } catch (NoSuchMethodException ignored) {
                ignore(ignored);
            }
            return ClassLoaderUtil.newInstance(serviceClass, classLoader, className);
        } catch (Exception e) {
            logger.severe(e);
        }
        return null;
    }

    synchronized void shutdown(boolean terminate) {
        logger.finest("Stopping services...");
        final List<ManagedService> managedServices = getServices(ManagedService.class);
        // reverse order to stop CoreServices last.
        Collections.reverse(managedServices);
        services.clear();
        for (ManagedService service : managedServices) {
            shutdownService(service, terminate);
        }
    }

    private void shutdownService(final ManagedService service, final boolean terminate) {
        try {
            if (logger.isFinestEnabled()) {
                logger.finest("Shutting down service -> " + service);
            }
            service.shutdown(terminate);
        } catch (Throwable t) {
            logger.severe("Error while shutting down service[" + service + "]: " + t.getMessage(), t);
        }
    }

    private synchronized void registerService(String serviceName, Object service) {
        if (logger.isFinestEnabled()) {
            logger.finest("Registering service: '" + serviceName + "'");
        }
        final ServiceInfo serviceInfo = new ServiceInfo(serviceName, service);
        final ServiceInfo currentServiceInfo = services.putIfAbsent(serviceName, serviceInfo);
        if (currentServiceInfo != null) {
            logger.warning("Replacing " + currentServiceInfo + " with " + serviceInfo);
            if (currentServiceInfo.isCoreService()) {
                throw new HazelcastException("Can not replace a CoreService! Name: " + serviceName
                        + ", Service: " + currentServiceInfo.getService());
            }
            if (currentServiceInfo.isManagedService()) {
                shutdownService((ManagedService) currentServiceInfo.getService(), false);
            }
            services.put(serviceName, serviceInfo);
        }
    }

    ServiceInfo getServiceInfo(String serviceName) {
        return services.get(serviceName);
    }

    /**
     * Returns a list of services matching provided service class/interface.
     * <br></br>
     * <b>CoreServices will be placed at the beginning of the list.</b>
     */
    <S> List<S> getServices(Class<S> serviceClass) {
        final LinkedList<S> result = new LinkedList<S>();
        for (ServiceInfo serviceInfo : services.values()) {
            if (serviceInfo.isInstanceOf(serviceClass)) {
                final S service = (S) serviceInfo.getService();
                if (serviceInfo.isCoreService()) {
                    result.addFirst(service);
                } else {
                    result.addLast(service);
                }
            }
        }
        return result;
    }

    /**
     * Returns a list of services matching provided service class/interface.
     * <br></br>
     * <b>CoreServices will be placed at the beginning of the list.</b>
     */
    List<ServiceInfo> getServiceInfos(Class serviceClass) {
        final LinkedList<ServiceInfo> result = new LinkedList<ServiceInfo>();
        for (ServiceInfo serviceInfo : services.values()) {
            if (serviceInfo.isInstanceOf(serviceClass)) {
                if (serviceInfo.isCoreService()) {
                    result.addFirst(serviceInfo);
                } else {
                    result.addLast(serviceInfo);
                }
            }
        }
        return result;
    }
}
