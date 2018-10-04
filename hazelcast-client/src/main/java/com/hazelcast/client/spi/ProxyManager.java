/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi;

import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.JCacheDetector;
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.client.ClientExtension;
import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.cache.impl.ClientCacheProxyFactory;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddDistributedObjectListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientCreateProxiesCodec;
import com.hazelcast.client.impl.protocol.codec.ClientCreateProxyCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemoveDistributedObjectListenerCodec;
import com.hazelcast.client.proxy.ClientAtomicLongProxy;
import com.hazelcast.client.proxy.ClientAtomicReferenceProxy;
import com.hazelcast.client.proxy.ClientCardinalityEstimatorProxy;
import com.hazelcast.client.proxy.ClientCountDownLatchProxy;
import com.hazelcast.client.proxy.ClientDurableExecutorServiceProxy;
import com.hazelcast.client.proxy.ClientExecutorServiceProxy;
import com.hazelcast.client.proxy.ClientFlakeIdGeneratorProxy;
import com.hazelcast.client.proxy.ClientIdGeneratorProxy;
import com.hazelcast.client.proxy.ClientListProxy;
import com.hazelcast.client.proxy.ClientLockProxy;
import com.hazelcast.client.proxy.ClientMapReduceProxy;
import com.hazelcast.client.proxy.ClientMultiMapProxy;
import com.hazelcast.client.proxy.ClientPNCounterProxy;
import com.hazelcast.client.proxy.ClientQueueProxy;
import com.hazelcast.client.proxy.ClientReliableTopicProxy;
import com.hazelcast.client.proxy.ClientReplicatedMapProxy;
import com.hazelcast.client.proxy.ClientRingbufferProxy;
import com.hazelcast.client.proxy.ClientScheduledExecutorProxy;
import com.hazelcast.client.proxy.ClientSemaphoreProxy;
import com.hazelcast.client.proxy.ClientSetProxy;
import com.hazelcast.client.proxy.ClientTopicProxy;
import com.hazelcast.client.proxy.txn.xa.XAResourceProxy;
import com.hazelcast.client.spi.impl.AbstractClientInvocationService;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientProxyFactoryWithContext;
import com.hazelcast.client.spi.impl.ClientServiceNotFoundException;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.spi.impl.listener.LazyDistributedObjectEvent;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.concurrent.lock.LockServiceImpl;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.Member;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.crdt.pncounter.PNCounterService;
import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Connection;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;
import com.hazelcast.transaction.impl.xa.XAService;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.ServiceLoader.classIterator;
import static java.lang.Thread.currentThread;

/**
 * The ProxyManager handles client proxy instantiation and retrieval at start and runtime by registering
 * corresponding service manager names and their {@link com.hazelcast.client.spi.ClientProxyFactory}s.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity",
        "checkstyle:classdataabstractioncoupling", "checkstyle:methodcount"})
public final class ProxyManager {

    private static final String PROVIDER_ID = ClientProxyDescriptorProvider.class.getCanonicalName();
    private static final Class[] LEGACY_CONSTRUCTOR_ARGUMENT_TYPES = new Class[]{String.class, String.class};
    private static final Class[] CONSTRUCTOR_ARGUMENT_TYPES = new Class[]{String.class, String.class, ClientContext.class};

    private final ConcurrentMap<String, ClientProxyFactory> proxyFactories = new ConcurrentHashMap<String, ClientProxyFactory>();
    private final ConcurrentMap<ObjectNamespace, ClientProxyFuture> proxies
            = new ConcurrentHashMap<ObjectNamespace, ClientProxyFuture>();

    private final ListenerMessageCodec distributedObjectListenerCodec = new ListenerMessageCodec() {
        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return ClientAddDistributedObjectListenerCodec.encodeRequest(localOnly);
        }

        @Override
        public String decodeAddResponse(ClientMessage clientMessage) {
            return ClientAddDistributedObjectListenerCodec.decodeResponse(clientMessage).response;
        }

        @Override
        public ClientMessage encodeRemoveRequest(String realRegistrationId) {
            return ClientRemoveDistributedObjectListenerCodec.encodeRequest(realRegistrationId);
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return ClientRemoveDistributedObjectListenerCodec.decodeResponse(clientMessage).response;
        }
    };

    private final HazelcastClientInstanceImpl client;

    private ClientContext context;
    private long invocationRetryPauseMillis;
    private long invocationTimeoutMillis;


    public ProxyManager(HazelcastClientInstanceImpl client) {
        this.client = client;

        List<ListenerConfig> listenerConfigs = client.getClientConfig().getListenerConfigs();
        if (listenerConfigs != null && !listenerConfigs.isEmpty()) {
            for (ListenerConfig listenerConfig : listenerConfigs) {
                if (listenerConfig.getImplementation() instanceof DistributedObjectListener) {
                    addDistributedObjectListener((DistributedObjectListener) listenerConfig.getImplementation());
                }
            }
        }
    }

    @SuppressWarnings("checkstyle:methodlength")
    public void init(ClientConfig config, ClientContext clientContext) {
        context = clientContext;
        // register defaults
        register(MapService.SERVICE_NAME, createServiceProxyFactory(MapService.class));
        if (JCacheDetector.isJCacheAvailable(config.getClassLoader())) {
            register(ICacheService.SERVICE_NAME, new ClientCacheProxyFactory(client));
        }
        register(QueueService.SERVICE_NAME, ClientQueueProxy.class);
        register(MultiMapService.SERVICE_NAME, ClientMultiMapProxy.class);
        register(ListService.SERVICE_NAME, ClientListProxy.class);
        register(SetService.SERVICE_NAME, ClientSetProxy.class);
        register(SemaphoreService.SERVICE_NAME, ClientSemaphoreProxy.class);
        register(TopicService.SERVICE_NAME, ClientTopicProxy.class);
        register(AtomicLongService.SERVICE_NAME, ClientAtomicLongProxy.class);
        register(AtomicReferenceService.SERVICE_NAME, ClientAtomicReferenceProxy.class);
        register(DistributedExecutorService.SERVICE_NAME, ClientExecutorServiceProxy.class);
        register(DistributedDurableExecutorService.SERVICE_NAME, ClientDurableExecutorServiceProxy.class);
        register(LockServiceImpl.SERVICE_NAME, ClientLockProxy.class);
        register(CountDownLatchService.SERVICE_NAME, ClientCountDownLatchProxy.class);
        register(MapReduceService.SERVICE_NAME, ClientMapReduceProxy.class);
        register(ReplicatedMapService.SERVICE_NAME, ClientReplicatedMapProxy.class);
        register(XAService.SERVICE_NAME, XAResourceProxy.class);
        register(RingbufferService.SERVICE_NAME, ClientRingbufferProxy.class);
        register(ReliableTopicService.SERVICE_NAME, new ClientProxyFactoryWithContext() {
            @Override
            public ClientProxy create(String id, ClientContext context) {
                return new ClientReliableTopicProxy(id, context, client);
            }
        });
        register(IdGeneratorService.SERVICE_NAME, new ClientProxyFactoryWithContext() {
            @Override
            public ClientProxy create(String id, ClientContext context) {
                IAtomicLong atomicLong = client.getAtomicLong(IdGeneratorService.ATOMIC_LONG_NAME + id);
                return new ClientIdGeneratorProxy(IdGeneratorService.SERVICE_NAME, id, context, atomicLong);
            }
        });
        register(FlakeIdGeneratorService.SERVICE_NAME, ClientFlakeIdGeneratorProxy.class);
        register(CardinalityEstimatorService.SERVICE_NAME, ClientCardinalityEstimatorProxy.class);
        register(DistributedScheduledExecutorService.SERVICE_NAME, ClientScheduledExecutorProxy.class);
        register(PNCounterService.SERVICE_NAME, ClientPNCounterProxy.class);

        ClassLoader classLoader = config.getClassLoader();
        for (ProxyFactoryConfig proxyFactoryConfig : config.getProxyFactoryConfigs()) {
            try {
                ClientProxyFactory clientProxyFactory = proxyFactoryConfig.getFactoryImpl();
                if (clientProxyFactory == null) {
                    String className = proxyFactoryConfig.getClassName();
                    clientProxyFactory = ClassLoaderUtil.newInstance(classLoader, className);
                }
                register(proxyFactoryConfig.getService(), clientProxyFactory);
            } catch (Exception e) {
                throw rethrow(e);
            }
        }

        readProxyDescriptors();
        AbstractClientInvocationService invocationService = (AbstractClientInvocationService) client.getInvocationService();
        invocationTimeoutMillis = invocationService.getInvocationTimeoutMillis();
        invocationRetryPauseMillis = invocationService.getInvocationRetryPauseMillis();
    }

    private void readProxyDescriptors() {
        try {
            ClassLoader classLoader = client.getClientConfig().getClassLoader();
            Iterator<Class<ClientProxyDescriptorProvider>> iter = classIterator(ClientProxyDescriptorProvider.class,
                    PROVIDER_ID, classLoader);

            while (iter.hasNext()) {
                Class<ClientProxyDescriptorProvider> clazz = iter.next();
                Constructor<ClientProxyDescriptorProvider> constructor = clazz.getDeclaredConstructor();
                ClientProxyDescriptorProvider provider = constructor.newInstance();
                ClientProxyDescriptor[] services = provider.createClientProxyDescriptors();

                for (ClientProxyDescriptor serviceDescriptor : services) {
                    register(serviceDescriptor.getServiceName(), serviceDescriptor.getClientProxyClass());
                }
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    /**
     * Creates a {@code ClientProxyFactory} for the supplied service class. Currently only the {@link MapService} is supported.
     *
     * @param service service for the proxy to create.
     * @return {@code ClientProxyFactory} for the service.
     * @throws java.lang.IllegalArgumentException if service is not known. Currently only the {@link MapService} is known
     */
    private <T> ClientProxyFactory createServiceProxyFactory(Class<T> service) {
        ClientExtension clientExtension = client.getClientExtension();
        return clientExtension.createServiceProxyFactory(service);
    }

    public ClientContext getContext() {
        return context;
    }

    public HazelcastInstance getHazelcastInstance() {
        return client;
    }

    public ClientProxyFactory getClientProxyFactory(String serviceName) {
        return proxyFactories.get(serviceName);
    }

    public void register(String serviceName, ClientProxyFactory factory) {
        if (proxyFactories.putIfAbsent(serviceName, factory) != null) {
            throw new IllegalArgumentException("Factory for service " + serviceName + " is already registered!");
        }
    }

    public void register(final String serviceName, final Class<? extends ClientProxy> proxyType) {
        try {
            register(serviceName, new ClientProxyFactoryWithContext() {
                @Override
                public ClientProxy create(String id, ClientContext context) {
                    return instantiateClientProxy(proxyType, serviceName, context, id);
                }
            });
        } catch (Exception e) {
            throw new HazelcastException("Factory for service " + serviceName + " could not be created for " + proxyType, e);
        }
    }

    public ClientProxy getOrCreateProxy(String service, String id) {
        final ObjectNamespace ns = new DistributedObjectNamespace(service, id);
        ClientProxyFuture proxyFuture = proxies.get(ns);
        if (proxyFuture != null) {
            return proxyFuture.get();
        }
        ClientProxyFactory factory = proxyFactories.get(service);
        if (factory == null) {
            throw new ClientServiceNotFoundException("No factory registered for service: " + service);
        }
        proxyFuture = new ClientProxyFuture();
        ClientProxyFuture current = proxies.putIfAbsent(ns, proxyFuture);
        if (current != null) {
            return current.get();
        }

        try {
            ClientProxy clientProxy = createClientProxy(id, factory);
            initializeWithRetry(clientProxy);
            proxyFuture.set(clientProxy);
            return clientProxy;
        } catch (Throwable e) {
            proxies.remove(ns);
            proxyFuture.set(e);
            throw rethrow(e);
        }
    }

    /**
     * Destroys the given proxy in a cluster-wide way.
     * <p>
     * Upon successful completion the proxy is unregistered in this proxy
     * manager, all local resources associated with the proxy are released and
     * a distributed object destruction operation is issued to the cluster.
     * <p>
     * If the given proxy instance is not registered in this proxy manager, the
     * proxy instance is considered stale. In this case, this stale instance is
     * a subject to a local-only destruction and its registered counterpart, if
     * there is any, is a subject to a cluster-wide destruction.
     *
     * @param proxy the proxy to destroy.
     */
    public void destroyProxy(ClientProxy proxy) {
        ObjectNamespace objectNamespace = new DistributedObjectNamespace(proxy.getServiceName(),
                proxy.getDistributedObjectName());
        ClientProxyFuture registeredProxyFuture = proxies.remove(objectNamespace);
        ClientProxy registeredProxy = registeredProxyFuture == null ? null : registeredProxyFuture.get();

        try {
            if (registeredProxy != null) {
                try {
                    registeredProxy.destroyLocally();
                } finally {
                    registeredProxy.destroyRemotely();
                }
            }
        } finally {
            if (proxy != registeredProxy) {
                // The given proxy is stale and was already destroyed, but the caller
                // may have allocated local resources in the context of this stale proxy
                // instance after it was destroyed, so we have to cleanup it locally one
                // more time to make sure there are no leaking local resources.
                proxy.destroyLocally();
            }
        }
    }

    /**
     * Locally destroys the proxy identified by the given service and object ID.
     * <p>
     * Upon successful completion the proxy is unregistered in this proxy
     * manager and all local resources associated with the proxy are released.
     *
     * @param service the service associated with the proxy.
     * @param id      the ID of the object to destroy the proxy of.
     */
    public void destroyProxyLocally(String service, String id) {
        ObjectNamespace objectNamespace = new DistributedObjectNamespace(service, id);
        ClientProxyFuture clientProxyFuture = proxies.remove(objectNamespace);
        if (clientProxyFuture != null) {
            ClientProxy clientProxy = clientProxyFuture.get();
            clientProxy.destroyLocally();
        }
    }

    private ClientProxy createClientProxy(String id, ClientProxyFactory factory) {
        if (factory instanceof ClientProxyFactoryWithContext) {
            return ((ClientProxyFactoryWithContext) factory).create(id, context);
        }
        return factory.create(id)
                .setContext(context);
    }

    private void initializeWithRetry(ClientProxy clientProxy) throws Exception {
        long startMillis = System.currentTimeMillis();
        while (System.currentTimeMillis() < startMillis + invocationTimeoutMillis) {
            try {
                initialize(clientProxy);
                return;
            } catch (Exception e) {
                boolean retryable = isRetryable(e);

                if (!retryable && e instanceof ExecutionException) {
                    retryable = isRetryable(e.getCause());
                }

                if (retryable) {
                    try {
                        Thread.sleep(invocationRetryPauseMillis);
                    } catch (InterruptedException ignored) {
                        currentThread().interrupt();
                    }
                } else {
                    throw e;
                }
            }
        }
        long elapsedTime = System.currentTimeMillis() - startMillis;
        throw new OperationTimeoutException("Initializing  " + clientProxy.getServiceName() + ":"
                + clientProxy.getName() + " is timed out after " + elapsedTime
                + " ms. Configured invocation timeout is " + invocationTimeoutMillis + " ms");
    }

    private boolean isRetryable(final Throwable t) {
        return ClientInvocation.isRetrySafeException(t);
    }

    private void initialize(ClientProxy clientProxy) throws Exception {
        Address initializationTarget = findNextAddressToSendCreateRequest();
        if (initializationTarget == null) {
            throw new IOException("Not able to find a member to create proxy on!");
        }
        ClientMessage clientMessage = ClientCreateProxyCodec.encodeRequest(clientProxy.getDistributedObjectName(),
                clientProxy.getServiceName(), initializationTarget);
        new ClientInvocation(client, clientMessage, clientProxy.getServiceName(), initializationTarget).invoke().get();
        clientProxy.onInitialize();
    }

    public Address findNextAddressToSendCreateRequest() {
        int clusterSize = client.getClientClusterService().getSize();
        if (clusterSize == 0) {
            throw new HazelcastClientOfflineException("Client connecting to cluster");
        }
        Member liteMember = null;

        final LoadBalancer loadBalancer = client.getLoadBalancer();
        for (int i = 0; i < clusterSize; i++) {
            Member member = loadBalancer.next();
            if (member != null && !member.isLiteMember()) {
                return member.getAddress();
            } else if (liteMember == null) {
                liteMember = member;
            }
        }

        return liteMember != null ? liteMember.getAddress() : null;
    }

    public Collection<? extends DistributedObject> getDistributedObjects() {
        Collection<DistributedObject> objects = new LinkedList<DistributedObject>();
        for (ClientProxyFuture future : proxies.values()) {
            objects.add(future.get());
        }
        return objects;
    }

    public void destroy() {
        for (ClientProxyFuture future : proxies.values()) {
            future.get().onShutdown();
        }
        proxies.clear();
    }

    public String addDistributedObjectListener(final DistributedObjectListener listener) {
        final EventHandler<ClientMessage> eventHandler = new DistributedObjectEventHandler(listener, this);
        return client.getListenerService().registerListener(distributedObjectListenerCodec, eventHandler);
    }

    public void createDistributedObjectsOnCluster(Connection ownerConnection) {
        List<Map.Entry<String, String>> proxyEntries = new LinkedList<Map.Entry<String, String>>();
        for (ObjectNamespace objectNamespace : proxies.keySet()) {
            String name = objectNamespace.getObjectName();
            String serviceName = objectNamespace.getServiceName();
            proxyEntries.add(new AbstractMap.SimpleEntry<String, String>(name, serviceName));
        }
        if (proxyEntries.isEmpty()) {
            return;
        }
        ClientMessage clientMessage = ClientCreateProxiesCodec.encodeRequest(proxyEntries);
        new ClientInvocation(client, clientMessage, null, ownerConnection).invokeUrgent();
        createCachesOnCluster();
    }

    private void createCachesOnCluster() {
        ClientCacheProxyFactory proxyFactory = (ClientCacheProxyFactory) getClientProxyFactory(ICacheService.SERVICE_NAME);
        if (proxyFactory != null) {
            proxyFactory.recreateCachesOnCluster();
        }
    }

    private final class DistributedObjectEventHandler extends ClientAddDistributedObjectListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final DistributedObjectListener listener;
        private ProxyManager proxyManager;

        private DistributedObjectEventHandler(DistributedObjectListener listener, ProxyManager proxyManager) {
            this.listener = listener;
            this.proxyManager = proxyManager;
        }

        @Override
        public void handleDistributedObjectEventV10(String name, String serviceName, String eventTypeName) {
            final ObjectNamespace ns = new DistributedObjectNamespace(serviceName, name);
            ClientProxyFuture future = proxies.get(ns);
            ClientProxy proxy = future == null ? null : future.get();
            DistributedObjectEvent.EventType eventType = DistributedObjectEvent.EventType.valueOf(eventTypeName);
            LazyDistributedObjectEvent event = new LazyDistributedObjectEvent(eventType, serviceName, name, proxy,
                    proxyManager);
            if (DistributedObjectEvent.EventType.CREATED.equals(eventType)) {
                listener.distributedObjectCreated(event);
            } else if (DistributedObjectEvent.EventType.DESTROYED.equals(eventType)) {
                listener.distributedObjectDestroyed(event);
            }
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }

    public boolean removeDistributedObjectListener(String id) {
        return client.getListenerService().deregisterListener(id);
    }

    private static class ClientProxyFuture {

        volatile Object proxy;

        ClientProxy get() {
            if (proxy == null) {
                boolean interrupted = false;
                synchronized (this) {
                    while (proxy == null) {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            interrupted = true;
                        }
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            if (proxy instanceof Throwable) {
                throw rethrow((Throwable) proxy);
            }
            return (ClientProxy) proxy;
        }

        void set(Object o) {
            if (o == null) {
                throw new IllegalArgumentException();
            }
            synchronized (this) {
                proxy = o;
                notifyAll();
            }
        }
    }

    private <T> T instantiateClientProxy(Class<T> proxyType, String serviceName, ClientContext context, String id) {
        try {
            try {
                Constructor<T> constructor = proxyType.getConstructor(CONSTRUCTOR_ARGUMENT_TYPES);
                return constructor.newInstance(serviceName, id, context);
            } catch (NoSuchMethodException e) {
                Constructor<T> constructor = proxyType.getConstructor(LEGACY_CONSTRUCTOR_ARGUMENT_TYPES);
                return constructor.newInstance(serviceName, id);
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
