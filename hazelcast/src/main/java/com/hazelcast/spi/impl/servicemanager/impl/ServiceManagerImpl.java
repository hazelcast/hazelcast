/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.servicemanager.impl;

import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.JCacheDetector;
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.reliableidgen.impl.ReliableIdGeneratorService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockServiceImpl;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.quorum.impl.QuorumServiceImpl;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.SharedService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.spi.impl.servicemanager.RemoteServiceDescriptor;
import com.hazelcast.spi.impl.servicemanager.RemoteServiceDescriptorProvider;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;
import com.hazelcast.transaction.impl.xa.XAService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationService;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.EmptyStatement.ignore;

@SuppressWarnings("checkstyle:classfanoutcomplexity")
public final class ServiceManagerImpl implements ServiceManager {
    private static final String PROVIDER_ID = "com.hazelcast.spi.impl.servicemanager.RemoteServiceDescriptorProvider";
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final ConcurrentMap<String, ServiceInfo> services = new ConcurrentHashMap<String, ServiceInfo>(20, .75f, 1);

    public ServiceManagerImpl(final NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ServiceManagerImpl.class);
    }

    public synchronized void start() {
        Map<String, Properties> serviceProps = new HashMap<String, Properties>();
        Map<String, Object> serviceConfigObjects = new HashMap<String, Object>();

        registerServices(serviceProps, serviceConfigObjects);
        initServices(serviceProps, serviceConfigObjects);
    }

    private void registerServices(Map<String, Properties> serviceProps, Map<String, Object> serviceConfigObjects) {
        registerCoreServices();
        registerExtensionServices();

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
        registerService(QuorumServiceImpl.SERVICE_NAME, nodeEngine.getQuorumService());
        registerService(WanReplicationService.SERVICE_NAME, nodeEngine.getWanReplicationService());
        registerService(EventServiceImpl.SERVICE_NAME, nodeEngine.getEventService());
    }

    private void registerExtensionServices() {
        logger.finest("Registering extension services...");
        NodeExtension nodeExtension = nodeEngine.getNode().getNodeExtension();
        Map<String, Object> services = nodeExtension.createExtensionServices();
        for (Map.Entry<String, Object> entry : services.entrySet()) {
            registerService(entry.getKey(), entry.getValue());
        }
    }

    private void registerDefaultServices(ServicesConfig servicesConfig) {
        if (!servicesConfig.isEnableDefaults()) {
            return;
        }

        logger.finest("Registering default services...");
        registerService(MapService.SERVICE_NAME, createService(MapService.class));
        registerService(LockService.SERVICE_NAME, new LockServiceImpl(nodeEngine));
        registerService(QueueService.SERVICE_NAME, new QueueService(nodeEngine));
        registerService(TopicService.SERVICE_NAME, new TopicService());
        registerService(ReliableTopicService.SERVICE_NAME, new ReliableTopicService(nodeEngine));
        registerService(MultiMapService.SERVICE_NAME, new MultiMapService(nodeEngine));
        registerService(ListService.SERVICE_NAME, new ListService(nodeEngine));
        registerService(SetService.SERVICE_NAME, new SetService(nodeEngine));
        registerService(DistributedExecutorService.SERVICE_NAME, new DistributedExecutorService());
        registerService(DistributedDurableExecutorService.SERVICE_NAME, new DistributedDurableExecutorService(nodeEngine));
        registerService(AtomicLongService.SERVICE_NAME, new AtomicLongService());
        registerService(AtomicReferenceService.SERVICE_NAME, new AtomicReferenceService());
        registerService(CountDownLatchService.SERVICE_NAME, new CountDownLatchService());
        registerService(SemaphoreService.SERVICE_NAME, new SemaphoreService(nodeEngine));
        registerService(IdGeneratorService.SERVICE_NAME, new IdGeneratorService(nodeEngine));
        registerService(ReliableIdGeneratorService.SERVICE_NAME, new ReliableIdGeneratorService(nodeEngine));
        registerService(MapReduceService.SERVICE_NAME, new MapReduceService(nodeEngine));
        registerService(ReplicatedMapService.SERVICE_NAME, new ReplicatedMapService(nodeEngine));
        registerService(RingbufferService.SERVICE_NAME, new RingbufferService(nodeEngine));
        registerService(XAService.SERVICE_NAME, new XAService(nodeEngine));
        registerService(CardinalityEstimatorService.SERVICE_NAME, new CardinalityEstimatorService());
        registerService(DistributedScheduledExecutorService.SERVICE_NAME, new DistributedScheduledExecutorService());
        registerCacheServiceIfAvailable();
        readServiceDescriptors();
    }

    private void readServiceDescriptors() {
        Node node = nodeEngine.getNode();

        try {
            ClassLoader classLoader = node.getConfigClassLoader();
            Iterator<Class<RemoteServiceDescriptorProvider>> iter =
                    com.hazelcast.util.ServiceLoader.classIterator(RemoteServiceDescriptorProvider.class, PROVIDER_ID,
                            classLoader);

            while (iter.hasNext()) {
                Class<RemoteServiceDescriptorProvider> clazz = iter.next();
                Constructor<RemoteServiceDescriptorProvider> constructor = clazz.getDeclaredConstructor();
                RemoteServiceDescriptorProvider provider = constructor.newInstance();
                RemoteServiceDescriptor[] services = provider.createRemoteServiceDescriptors();

                for (RemoteServiceDescriptor serviceDescriptor : services) {
                    registerService(serviceDescriptor.getServiceName(), serviceDescriptor.getService(nodeEngine));
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private <T> T createService(Class<T> service) {
        Node node = nodeEngine.getNode();
        NodeExtension nodeExtension = node.getNodeExtension();
        return nodeExtension.createService(service);
    }

    private void registerCacheServiceIfAvailable() {
        //CacheService Optional initialization
        //search for jcache api jar on classpath
        if (JCacheDetector.isJCacheAvailable(nodeEngine.getConfigClassLoader(), logger)) {
            ICacheService service = createService(ICacheService.class);
            registerService(ICacheService.SERVICE_NAME, service);
        } else {
            logger.finest("javax.cache api is not detected on classpath. Skipping CacheService...");
        }
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

        Object service = serviceConfig.getImplementation();
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

    public synchronized void shutdown(boolean terminate) {
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

    public synchronized void registerService(String serviceName, Object service) {
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

    @Override
    public ServiceInfo getServiceInfo(String serviceName) {
        return services.get(serviceName);
    }

    @Override
    public <S> List<S> getServices(Class<S> serviceClass) {
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

    @Override
    public <T> T getService(String serviceName) {
        final ServiceInfo serviceInfo = getServiceInfo(serviceName);
        return serviceInfo != null ? (T) serviceInfo.getService() : null;
    }

    @Override
    public <T extends SharedService> T getSharedService(String serviceName) {
        final Object service = getService(serviceName);
        if (service == null) {
            return null;
        }

        if (service instanceof SharedService) {
            return (T) service;
        }

        throw new IllegalArgumentException("No SharedService registered with name: " + serviceName);
    }

    /**
     * Returns a list of services matching provided service class/interface.
     * <br></br>
     * <b>CoreServices will be placed at the beginning of the list.</b>
     */
    @Override
    public List<ServiceInfo> getServiceInfos(Class serviceClass) {
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
