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

package com.hazelcast.client.spi;

import com.hazelcast.client.ClientCreateRequest;
import com.hazelcast.client.DistributedObjectListenerRequest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.RemoveDistributedObjectListenerRequest;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.client.proxy.ClientAtomicLongProxy;
import com.hazelcast.client.proxy.ClientAtomicReferenceProxy;
import com.hazelcast.client.proxy.ClientCountDownLatchProxy;
import com.hazelcast.client.proxy.ClientExecutorServiceProxy;
import com.hazelcast.client.proxy.ClientIdGeneratorProxy;
import com.hazelcast.client.proxy.ClientListProxy;
import com.hazelcast.client.proxy.ClientLockProxy;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.proxy.ClientMapReduceProxy;
import com.hazelcast.client.proxy.ClientMultiMapProxy;
import com.hazelcast.client.proxy.ClientQueueProxy;
import com.hazelcast.client.proxy.ClientSemaphoreProxy;
import com.hazelcast.client.proxy.ClientSetProxy;
import com.hazelcast.client.proxy.ClientTopicProxy;
import com.hazelcast.client.util.ListenerUtil;
import com.hazelcast.collection.list.ListService;
import com.hazelcast.collection.set.SetService;
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
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.MapService;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.impl.PortableDistributedObjectEvent;
import com.hazelcast.topic.TopicService;
import com.hazelcast.util.ExceptionUtil;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author mdogan 5/16/13
 */
public final class ProxyManager {

    private final static ILogger logger = Logger.getLogger(ProxyManager.class);

    private final HazelcastClient client;
    private final ConcurrentMap<String, ClientProxyFactory> proxyFactories = new ConcurrentHashMap<String, ClientProxyFactory>();
    private final ConcurrentMap<ObjectNamespace, ClientProxyFuture> proxies = new ConcurrentHashMap<ObjectNamespace, ClientProxyFuture>();

    public ProxyManager(HazelcastClient client) {
        this.client = client;
        final List<ListenerConfig> listenerConfigs = client.getClientConfig().getListenerConfigs();
        if(listenerConfigs != null && !listenerConfigs.isEmpty()){
            for (ListenerConfig listenerConfig : listenerConfigs) {
                if (listenerConfig.getImplementation() instanceof DistributedObjectListener) {
                    addDistributedObjectListener((DistributedObjectListener) listenerConfig.getImplementation());
                }
            }
        }
    }

    public void init(ClientConfig config) {
        final String instanceName = client.getName();
        // register defaults
        register(MapService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientMapProxy(instanceName, MapService.SERVICE_NAME, String.valueOf(id));
            }
        });
        register(QueueService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientQueueProxy(instanceName, QueueService.SERVICE_NAME, String.valueOf(id));
            }
        });
        register(MultiMapService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientMultiMapProxy(instanceName, MultiMapService.SERVICE_NAME, String.valueOf(id));
            }
        });
        register(ListService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientListProxy(instanceName, ListService.SERVICE_NAME, String.valueOf(id));
            }
        });
        register(SetService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientSetProxy(instanceName, SetService.SERVICE_NAME, String.valueOf(id));
            }
        });
        register(SemaphoreService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientSemaphoreProxy(instanceName, SemaphoreService.SERVICE_NAME, String.valueOf(id));
            }
        });
        register(TopicService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientTopicProxy(instanceName, TopicService.SERVICE_NAME, String.valueOf(id));
            }
        });
        register(AtomicLongService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientAtomicLongProxy(instanceName, AtomicLongService.SERVICE_NAME, String.valueOf(id));
            }
        });
        register(AtomicReferenceService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientAtomicReferenceProxy(instanceName, AtomicReferenceService.SERVICE_NAME, String.valueOf(id));
            }
        });
        register(DistributedExecutorService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientExecutorServiceProxy(instanceName, DistributedExecutorService.SERVICE_NAME, String.valueOf(id));
            }
        });
        register(LockServiceImpl.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientLockProxy(instanceName, LockServiceImpl.SERVICE_NAME, id);
            }
        });
        register(IdGeneratorService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                String name = String.valueOf(id);
                IAtomicLong atomicLong = client.getAtomicLong(IdGeneratorService.ATOMIC_LONG_NAME + name);
                return new ClientIdGeneratorProxy(instanceName, IdGeneratorService.SERVICE_NAME, name, atomicLong);
            }
        });
        register(CountDownLatchService.SERVICE_NAME, new ClientProxyFactory() {
            public ClientProxy create(String id) {
                return new ClientCountDownLatchProxy(instanceName, CountDownLatchService.SERVICE_NAME, String.valueOf(id));
            }
        });
        register(MapReduceService.SERVICE_NAME, new ClientProxyFactory() {
            @Override
            public ClientProxy create(String id) {
                return new ClientMapReduceProxy(instanceName, MapReduceService.SERVICE_NAME, id);
            }
        });

        for (ProxyFactoryConfig proxyFactoryConfig:config.getProxyFactoryConfigs()){
            try {
                ClientProxyFactory clientProxyFactory = ClassLoaderUtil.newInstance(config.getClassLoader(), proxyFactoryConfig.getClassName());
                register(proxyFactoryConfig.getService(),clientProxyFactory);
            } catch (Exception e) {
                logger.severe(e);
            }
        }
    }

    public void register(String serviceName, ClientProxyFactory factory) {
        if (proxyFactories.putIfAbsent(serviceName, factory) != null) {
            throw new IllegalArgumentException("Factory for service: " + serviceName + " is already registered!");
        }
    }

    public ClientProxy getProxy(String service, String id) {
        final ObjectNamespace ns = new DefaultObjectNamespace(service, id);
        ClientProxyFuture proxyFuture = proxies.get(ns);
        if (proxyFuture != null) {
            return proxyFuture.get();
        }
        final ClientProxyFactory factory = proxyFactories.get(service);
        if (factory == null) {
            throw new IllegalArgumentException("No factory registered for service: " + service);
        }
        final ClientProxy clientProxy = factory.create(id);
        proxyFuture = new ClientProxyFuture();
        final ClientProxyFuture current = proxies.putIfAbsent(ns, proxyFuture);
        if (current != null) {
            return current.get();
        }
        try {
            initialize(clientProxy);
        } catch (Exception e) {
            proxies.remove(ns);
            proxyFuture.set(e);
            throw ExceptionUtil.rethrow(e);
        }
        proxyFuture.set(clientProxy);
        return clientProxy;
    }

    public ClientProxy removeProxy(String service, String id) {
        final ObjectNamespace ns = new DefaultObjectNamespace(service, id);
        return proxies.remove(ns).get();
    }

    private void initialize(ClientProxy clientProxy) throws Exception {
        ClientCreateRequest request = new ClientCreateRequest(clientProxy.getName(), clientProxy.getServiceName());
        client.getInvocationService().invokeOnRandomTarget(request).get();
        clientProxy.setContext(new ClientContext(client, this));
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
        final DistributedObjectListenerRequest request = new DistributedObjectListenerRequest();
        final EventHandler<PortableDistributedObjectEvent> eventHandler = new EventHandler<PortableDistributedObjectEvent>(){
            public void handle(PortableDistributedObjectEvent e) {
                final ObjectNamespace ns = new DefaultObjectNamespace(e.getServiceName(), e.getName());
                ClientProxyFuture future = proxies.get(ns);
                ClientProxy proxy = future == null ? null : future.get();
                if (proxy == null) {
                    proxy = getProxy(e.getServiceName(), e.getName());
                }

                DistributedObjectEvent event = new DistributedObjectEvent(e.getEventType(), e.getServiceName(), proxy);
                if (DistributedObjectEvent.EventType.CREATED.equals(e.getEventType())){
                    listener.distributedObjectCreated(event);
                } else if (DistributedObjectEvent.EventType.DESTROYED.equals(e.getEventType())){
                    listener.distributedObjectDestroyed(event);
                }
            }

            @Override
            public void onListenerRegister() {

            }
        };
        final ClientContext clientContext = new ClientContext(client, this);
        return ListenerUtil.listen(clientContext, request, null, eventHandler);
    }

    public boolean removeDistributedObjectListener(String id) {
        final RemoveDistributedObjectListenerRequest request = new RemoveDistributedObjectListenerRequest(id);
        final ClientContext clientContext = new ClientContext(client, this);
        return ListenerUtil.stopListening(clientContext, request, id);
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
                throw ExceptionUtil.rethrow((Throwable)proxy);
            }
            return (ClientProxy)proxy;
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
}
