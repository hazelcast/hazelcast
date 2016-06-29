/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.ClientExtension;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.cache.impl.ClientCacheProxyFactory;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddDistributedObjectListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientCreateProxyCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemoveDistributedObjectListenerCodec;
import com.hazelcast.client.proxy.ClientAtomicLongProxy;
import com.hazelcast.client.proxy.ClientAtomicReferenceProxy;
import com.hazelcast.client.proxy.ClientCountDownLatchProxy;
import com.hazelcast.client.proxy.ClientDurableExecutorServiceProxy;
import com.hazelcast.client.proxy.ClientExecutorServiceProxy;
import com.hazelcast.client.proxy.ClientIdGeneratorProxy;
import com.hazelcast.client.proxy.ClientListProxy;
import com.hazelcast.client.proxy.ClientLockProxy;
import com.hazelcast.client.proxy.ClientMapReduceProxy;
import com.hazelcast.client.proxy.ClientMultiMapProxy;
import com.hazelcast.client.proxy.ClientQueueProxy;
import com.hazelcast.client.proxy.ClientReliableTopicProxy;
import com.hazelcast.client.proxy.ClientReplicatedMapProxy;
import com.hazelcast.client.proxy.ClientRingbufferProxy;
import com.hazelcast.client.proxy.ClientSemaphoreProxy;
import com.hazelcast.client.proxy.ClientSetProxy;
import com.hazelcast.client.proxy.ClientTopicProxy;
import com.hazelcast.client.proxy.txn.xa.XAResourceProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
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
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Connection;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;
import com.hazelcast.transaction.impl.xa.XAService;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.spi.properties.ClientProperty.INVOCATION_TIMEOUT_SECONDS;

/**
 * The ProxyManager handles client proxy instantiation and retrieval at start- and runtime by registering
 * corresponding service manager names and their {@link com.hazelcast.client.spi.ClientProxyFactory}s.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public final class ProxyManager {
    private static final String PROVIDER_ID = "com.hazelcast.client.spi.ClientProxyDescriptorProvider";
    private static final Class[] CONSTRUCTOR_ARGUMENT_TYPES = new Class[]{String.class, String.class};

    private final HazelcastClientInstanceImpl client;
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

    public ProxyManager(HazelcastClientInstanceImpl client) {
        this.client = client;
        final List<ListenerConfig> listenerConfigs = client.getClientConfig().getListenerConfigs();
        if (listenerConfigs != null && !listenerConfigs.isEmpty()) {
            for (ListenerConfig listenerConfig : listenerConfigs) {
                if (listenerConfig.getImplementation() instanceof DistributedObjectListener) {
                    addDistributedObjectListener((DistributedObjectListener) listenerConfig.getImplementation());
                }
            }
        }
    }

    public void init(ClientConfig config) {
        // register defaults
        register(MapService.SERVICE_NAME, createServiceProxyFactory(MapService.class));
        if (JCacheDetector.isJcacheAvailable(config.getClassLoader())) {
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
        register(ReliableTopicService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientReliableTopicProxy(id, client);
            }
        });
        register(IdGeneratorService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                IAtomicLong atomicLong = client.getAtomicLong(IdGeneratorService.ATOMIC_LONG_NAME + id);
                return new ClientIdGeneratorProxy(IdGeneratorService.SERVICE_NAME, id, atomicLong);
            }
        });

        for (ProxyFactoryConfig proxyFactoryConfig : config.getProxyFactoryConfigs()) {
            try {
                ClassLoader classLoader = config.getClassLoader();
                ClientProxyFactory clientProxyFactory = proxyFactoryConfig.getFactoryImpl();
                if (clientProxyFactory == null) {
                    String className = proxyFactoryConfig.getClassName();
                    clientProxyFactory = ClassLoaderUtil.newInstance(classLoader, className);
                }
                register(proxyFactoryConfig.getService(), clientProxyFactory);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        readProxyDescriptors();
    }

    private void readProxyDescriptors() {
        try {
            ClassLoader classLoader = client.getClientConfig().getClassLoader();
            Iterator<Class<ClientProxyDescriptorProvider>> iter =
                    com.hazelcast.util.ServiceLoader.classIterator(PROVIDER_ID, classLoader);

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
            throw ExceptionUtil.rethrow(e);
        }
    }

    private <T> ClientProxyFactory createServiceProxyFactory(Class<T> service) {
        ClientExtension clientExtension = client.getClientExtension();
        return clientExtension.createServiceProxyFactory(service);
    }

    public HazelcastInstance getHazelcastInstance() {
        return client;
    }

    public void register(String serviceName, ClientProxyFactory factory) {
        if (proxyFactories.putIfAbsent(serviceName, factory) != null) {
            throw new IllegalArgumentException("Factory for service: " + serviceName + " is already registered!");
        }
    }

    public ClientProxyFactory getClientProxyFactory(String serviceName) {
        return proxyFactories.get(serviceName);
    }

    public void register(final String serviceName, final Class<? extends ClientProxy> proxyType) {
        try {
            register(serviceName, new ClientProxyFactory() {
                @Override
                public ClientProxy create(String id) {
                    return instantiateClientProxy(proxyType, serviceName, id);
                }
            });

        } catch (Exception e) {
            throw new HazelcastException("Could not initialize Proxy", e);
        }
    }

    public ClientProxy getOrCreateProxy(String service, String id) {
        final ObjectNamespace ns = new DefaultObjectNamespace(service, id);
        ClientProxyFuture proxyFuture = proxies.get(ns);
        if (proxyFuture != null) {
            return proxyFuture.get();
        }
        final ClientProxyFactory factory = proxyFactories.get(service);
        if (factory == null) {
            throw new ClientServiceNotFoundException("No factory registered for service: " + service);
        }
        final ClientProxy clientProxy = factory.create(id);
        proxyFuture = new ClientProxyFuture();
        final ClientProxyFuture current = proxies.putIfAbsent(ns, proxyFuture);
        if (current != null) {
            return current.get();
        }
        try {
            initializeWithRetry(clientProxy);
        } catch (Throwable e) {
            proxies.remove(ns);
            proxyFuture.set(e);
            throw ExceptionUtil.rethrow(e);
        }
        proxyFuture.set(clientProxy);
        return clientProxy;
    }

    public void removeProxy(String service, String id) {
        final ObjectNamespace ns = new DefaultObjectNamespace(service, id);
        proxies.remove(ns);
    }

    private void initializeWithRetry(ClientProxy clientProxy) throws Exception {
        final long retryCountLimit = getRetryCountLimit();
        for (int retryCount = 0; retryCount < retryCountLimit; retryCount++) {
            try {
                initialize(clientProxy);
                return;
            } catch (Exception e) {
                boolean retryable = isRetryable(e);

                if (!retryable && e instanceof ExecutionException) {
                    retryable = isRetryable(e.getCause());
                }

                if (retryable) {
                    sleepForProxyInitRetry();
                } else {
                    throw e;
                }
            }
        }
    }

    private long getRetryCountLimit() {
        HazelcastProperties hazelcastProperties = client.getProperties();
        int waitTime = hazelcastProperties.getSeconds(INVOCATION_TIMEOUT_SECONDS);
        long retryTimeoutInSeconds = waitTime > 0 ? waitTime : Integer.parseInt(INVOCATION_TIMEOUT_SECONDS.getDefaultValue());
        return retryTimeoutInSeconds / ClientInvocation.RETRY_WAIT_TIME_IN_SECONDS;
    }

    private boolean isRetryable(final Throwable t) {
        return t instanceof RetryableException
                || ClientInvocation.isRetryable(t);
    }

    private void sleepForProxyInitRetry() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(ClientInvocation.RETRY_WAIT_TIME_IN_SECONDS));
        } catch (InterruptedException ignored) {
            EmptyStatement.ignore(ignored);
        }
    }

    private void initialize(ClientProxy clientProxy) throws Exception {
        final Address initializationTarget = findNextAddressToSendCreateRequest();
        final Connection connection = getTargetOrOwnerConnection(initializationTarget);
        final ClientMessage clientMessage = ClientCreateProxyCodec.encodeRequest(clientProxy.getDistributedObjectName(),
                clientProxy.getServiceName(), initializationTarget);
        final ClientContext context = new ClientContext(client, this);
        new ClientInvocation(client, clientMessage, connection).invoke().get();
        clientProxy.setContext(context);
        clientProxy.onInitialize();
    }

    private Connection getTargetOrOwnerConnection(final Address target) throws IOException {
        if (target == null) {
            throw new IOException("Not able to setup owner connection!");
        }

        final ClientConnectionManager connectionManager = client.getConnectionManager();
        Connection connection = connectionManager.getConnection(target);
        if (connection == null) {
            final Address ownerConnectionAddress = client.getClientClusterService().getOwnerConnectionAddress();
            if (ownerConnectionAddress == null) {
                throw new IOException("Not able to setup owner connection!");
            }

            connection = connectionManager.getConnection(ownerConnectionAddress);
            if (connection == null) {
                throw new IOException("Client is not connected to member " + target);
            }
        }

        return connection;
    }

    public Address findNextAddressToSendCreateRequest() {
        int clusterSize = client.getClientClusterService().getSize();
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

    private final class DistributedObjectEventHandler extends ClientAddDistributedObjectListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final DistributedObjectListener listener;
        private ProxyManager proxyManager;

        private DistributedObjectEventHandler(DistributedObjectListener listener, ProxyManager proxyManager) {
            this.listener = listener;
            this.proxyManager = proxyManager;
        }

        @Override
        public void handle(String name, String serviceName, String eventTypeName) {
            final ObjectNamespace ns = new DefaultObjectNamespace(serviceName, name);
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
                throw ExceptionUtil.rethrow((Throwable) proxy);
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

    private <T> T instantiateClientProxy(Class<T> proxyType, String serviceName, String id) {
        try {
            final Constructor<T> constructor = proxyType.getConstructor(CONSTRUCTOR_ARGUMENT_TYPES);
            return constructor.newInstance(serviceName, id);

        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
