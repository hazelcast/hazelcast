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
import com.hazelcast.client.proxy.*;
import com.hazelcast.client.util.ListenerUtil;
import com.hazelcast.collection.list.ListService;
import com.hazelcast.collection.set.SetService;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
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
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.impl.PortableDistributedObjectEvent;
import com.hazelcast.topic.TopicService;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author mdogan 5/16/13
 */
public final class ProxyManager {

    private final static ILogger logger = Logger.getLogger(ProxyManager.class);

    private final HazelcastClient client;
    private final ConcurrentMap<String, ClientProxyFactory> proxyFactories = new ConcurrentHashMap<String, ClientProxyFactory>();
    private final ConcurrentMap<ObjectNamespace, ClientProxy> proxies = new ConcurrentHashMap<ObjectNamespace, ClientProxy>();

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
        final ClientProxy proxy = proxies.get(ns);
        if (proxy != null) {
            return proxy;
        }
        final ClientProxyFactory factory = proxyFactories.get(service);
        if (factory == null) {
            throw new IllegalArgumentException("No factory registered for service: " + service);
        }
        final ClientProxy clientProxy = factory.create(id);
        initialize(clientProxy);
        final ClientProxy current = proxies.putIfAbsent(ns, clientProxy);
        if (current != null){
            return current;
        }
        return clientProxy;
    }

    public ClientProxy removeProxy(String service, String id) {
        final ObjectNamespace ns = new DefaultObjectNamespace(service, id);
        return proxies.remove(ns);
    }

    private void initialize(ClientProxy clientProxy) {
        ClientCreateRequest request = new ClientCreateRequest(clientProxy.getName(), clientProxy.getServiceName());
        try {
            client.getInvocationService().invokeOnRandomTarget(request).get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        final ClientContext clientContext = new ClientContext(client, this);
        clientProxy.setContext(clientContext);
    }

    public Collection<? extends DistributedObject> getDistributedObjects(){
        return Collections.unmodifiableCollection(proxies.values());
    }

    public void destroy() {
        for (ClientProxy proxy : proxies.values()) {
            proxy.onShutdown();
        }
        proxies.clear();
    }

    public String addDistributedObjectListener(final DistributedObjectListener listener) {
        final DistributedObjectListenerRequest request = new DistributedObjectListenerRequest();
        final EventHandler<PortableDistributedObjectEvent> eventHandler = new EventHandler<PortableDistributedObjectEvent>(){
            public void handle(PortableDistributedObjectEvent e) {
                final ObjectNamespace ns = new DefaultObjectNamespace(e.getServiceName(), e.getName());
                ClientProxy proxy = proxies.get(ns);
                if (proxy == null){
                    proxy = getProxy(e.getServiceName(), e.getName());
                }
                DistributedObjectEvent event = new DistributedObjectEvent(e.getEventType(), e.getServiceName(), proxy);
                if (DistributedObjectEvent.EventType.CREATED.equals(e.getEventType())){
                    listener.distributedObjectCreated(event);
                } else if (DistributedObjectEvent.EventType.DESTROYED.equals(e.getEventType())){
                    listener.distributedObjectDestroyed(event);
                }
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
}
