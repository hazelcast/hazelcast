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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.executor.StripedRunnable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.DistributedObjectEvent.EventType;
import static com.hazelcast.core.DistributedObjectEvent.EventType.CREATED;
import static com.hazelcast.core.DistributedObjectEvent.EventType.DESTROYED;

/**
 * @author mdogan 1/11/13
 */
public class ProxyServiceImpl implements ProxyService, PostJoinAwareService,
        EventPublishingService<DistributedObjectEventPacket, Object> {

    static final String SERVICE_NAME = "hz:core:proxyService";

    private final NodeEngineImpl nodeEngine;
    private final ConcurrentMap<String, ProxyRegistry> registries = new ConcurrentHashMap<String, ProxyRegistry>();
    private final ConcurrentMap<String, DistributedObjectListener> listeners = new ConcurrentHashMap<String, DistributedObjectListener>();
    private final ILogger logger;

    ProxyServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ProxyService.class.getName());
    }

    void init() {
        nodeEngine.getEventService().registerListener(SERVICE_NAME, SERVICE_NAME, new Object());
    }

    private final ConstructorFunction<String, ProxyRegistry> registryConstructor
            = new ConstructorFunction<String, ProxyRegistry>() {
        public ProxyRegistry createNew(String serviceName) {
            return new ProxyRegistry(serviceName);
        }
    };

    @Override
    public int getProxyCount() {
        int count = 0;
        for (ProxyRegistry registry : registries.values()) {
            count += registry.getProxyCount();
        }

        return count;
    }

    public void initializeDistributedObject(String serviceName, Object objectId) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        if (objectId == null) {
            throw new NullPointerException("Object id is required!");
        }
        getDistributedObject(serviceName, objectId);
    }

    public DistributedObject getDistributedObject(String serviceName, Object objectId) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        if (objectId == null) {
            throw new NullPointerException("Object id is required!");
        }
        ProxyRegistry registry = ConcurrencyUtil.getOrPutIfAbsent(registries, serviceName, registryConstructor);
        return registry.getProxy(objectId, true);
    }

    public void destroyDistributedObject(String serviceName, Object objectId) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        if (objectId == null) {
            throw new NullPointerException("Object id is required!");
        }
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        Collection<Future> calls = new ArrayList<Future>(members.size());
        for (MemberImpl member : members) {
            if (member.localMember()) continue;

            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME,
                    new DistributedObjectDestroyOperation(serviceName, objectId), member.getAddress())
                    .setTryCount(10).build();
            calls.add(inv.invoke());
        }
        destroyLocalDistributedObject(serviceName, objectId);
        for (Future f : calls) {
            try {
                f.get(3, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.finest(e);
            }
        }
        ProxyRegistry registry = registries.get(serviceName);
        if (registry != null) {
            registry.destroyProxy(objectId, true);
        }
    }

    private void destroyLocalDistributedObject(String serviceName, Object objectId) {
        final RemoteService service = nodeEngine.getService(serviceName);
        if (service != null) {
            service.destroyDistributedObject(objectId);
        }
        nodeEngine.waitNotifyService.cancelWaitingOps(serviceName, objectId, new DistributedObjectDestroyedException(serviceName, objectId));
    }

    public Collection<DistributedObject> getDistributedObjects(String serviceName) {
        if (serviceName == null) {
            throw new NullPointerException("Service name is required!");
        }
        Collection<DistributedObject> objects = new LinkedList<DistributedObject>();
        ProxyRegistry registry = registries.get(serviceName);
        if (registry != null) {
            objects.addAll(registry.proxies.values());
        }
        return objects;
    }

    public Collection<DistributedObject> getAllDistributedObjects() {
        Collection<DistributedObject> objects = new LinkedList<DistributedObject>();
        for (ProxyRegistry registry : registries.values()) {
            objects.addAll(registry.proxies.values());
        }
        return objects;
    }

    public String addProxyListener(DistributedObjectListener distributedObjectListener) {
        final String id = UUID.randomUUID().toString();
        listeners.put(id, distributedObjectListener);
        return id;
    }

    public boolean removeProxyListener(String registrationId) {
        return listeners.remove(registrationId) != null;
    }

    public void dispatchEvent(final DistributedObjectEventPacket eventPacket, Object ignore) {
        final String serviceName = eventPacket.getServiceName();
        if (eventPacket.getEventType() == CREATED) {
            try {
                final ProxyRegistry registry = ConcurrencyUtil.getOrPutIfAbsent(registries, serviceName, registryConstructor);
                if (!registry.contains(eventPacket.getObjectId())) {
                    registry.getProxy(eventPacket.getObjectId(), false); // listeners will be called if proxy is created here.
                }
            } catch (HazelcastInstanceNotActiveException ignored) {
            }
        } else {
            final ProxyRegistry registry = registries.get(serviceName);
            if (registry != null) {
                registry.destroyProxy(eventPacket.getObjectId(), false);
            }
        }
    }

    public Operation getPostJoinOperation() {
        Collection<ProxyInfo> proxies = new LinkedList<ProxyInfo>();
        for (ProxyRegistry registry : registries.values()) {
            for (DistributedObject distributedObject : registry.proxies.values()) {
                if (distributedObject instanceof InitializingObject) {
                    proxies.add(new ProxyInfo(registry.serviceName, distributedObject.getId()));
                }
            }
        }
        return proxies.isEmpty() ? null : new PostJoinProxyOperation(proxies);
    }

    private class ProxyRegistry {

        final String serviceName;
        final RemoteService service;
        final ConcurrentMap<Object, DistributedObject> proxies = new ConcurrentHashMap<Object, DistributedObject>();

        private ProxyRegistry(String serviceName) {
            this.serviceName = serviceName;
            this.service = nodeEngine.getService(serviceName);
            if (service == null) {
                if (nodeEngine.isActive()) {
                    throw new IllegalArgumentException("Unknown service: " + serviceName);
                } else {
                    throw new HazelcastInstanceNotActiveException();
                }
            }
        }

        DistributedObject getProxy(final Object objectId, boolean publishEvent) {
            DistributedObject proxy = proxies.get(objectId);
            if (proxy == null) {
                if (!nodeEngine.isActive()) {
                    throw new HazelcastInstanceNotActiveException();
                }
                proxy = service.createDistributedObject(objectId);
                DistributedObject current = proxies.putIfAbsent(objectId, proxy);
                if (current == null) {
                    if (proxy instanceof InitializingObject) {
                        try {
                            ((InitializingObject) proxy).initialize();
                        } catch (Exception e) {
                            logger.warning("Error while initializing proxy: " + proxy, e);
                        }
                    }
                    nodeEngine.eventService.executeEvent(new ProxyEventProcessor(CREATED, serviceName, proxy));
                    if (publishEvent) {
                        publish(new DistributedObjectEventPacket(CREATED, serviceName, objectId));
                    }
                } else {
                    proxy = current;
                }
            }
            return proxy;
        }

        void destroyProxy(Object objectId, boolean publishEvent) {
            final DistributedObject proxy;
            if ((proxy = proxies.remove(objectId)) != null) {
                nodeEngine.eventService.executeEvent(new ProxyEventProcessor(DESTROYED, serviceName, proxy));
                if (publishEvent) {
                    publish(new DistributedObjectEventPacket(DESTROYED, serviceName, objectId));
                }
            }
        }

        private void publish(DistributedObjectEventPacket event) {
            final EventService eventService = nodeEngine.getEventService();
            final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
            eventService.publishEvent(SERVICE_NAME, registrations, event, event.getObjectId().hashCode());
        }

        private boolean contains(Object objectId) {
            return proxies.containsKey(objectId);
        }

        void destroy() {
            for (DistributedObject distributedObject : proxies.values()) {
                if (distributedObject instanceof AbstractDistributedObject) {
                    DistributedObjectAccessor.onNodeShutdown((AbstractDistributedObject) distributedObject);
                }
            }
            proxies.clear();
        }

        public int getProxyCount() {
            return proxies.size();
        }
    }

    private class ProxyEventProcessor implements StripedRunnable {

        final EventType type;
        final String serviceName;
        final DistributedObject object;

        private ProxyEventProcessor(EventType eventType, String serviceName, DistributedObject object) {
            this.type = eventType;
            this.serviceName = serviceName;
            this.object = object;
        }

        public void run() {
            DistributedObjectEvent event = new DistributedObjectEvent(type, serviceName, object);
            for (DistributedObjectListener listener : listeners.values()) {
                listener.distributedObjectCreated(event);
            }
        }

        public int getKey() {
            return object.getId().hashCode();
        }
    }

    public static class DistributedObjectDestroyOperation extends AbstractOperation {

        private String serviceName;
        private Object objectId;

        public DistributedObjectDestroyOperation() {
        }

        public DistributedObjectDestroyOperation(String serviceName, Object objectId) {
            this.serviceName = serviceName;
            this.objectId = objectId;
        }

        public void run() throws Exception {
            ProxyServiceImpl proxyService = getService();
            ProxyRegistry registry = proxyService.registries.get(serviceName);
            if (registry != null) {
                registry.destroyProxy(objectId, false);
            }
            proxyService.destroyLocalDistributedObject(serviceName, objectId);
        }

        public boolean returnsResponse(Throwable throwable) {
            return true;
        }

        public Object getResponse() {
            return Boolean.TRUE;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeUTF(serviceName);
            out.writeObject(objectId);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            serviceName = in.readUTF();
            objectId = in.readObject();
        }
    }

    public static class PostJoinProxyOperation extends AbstractOperation {

        private Collection<ProxyInfo> proxies;

        public PostJoinProxyOperation() {
        }

        public PostJoinProxyOperation(Collection<ProxyInfo> proxies) {
            this.proxies = proxies;
        }

        @Override
        public void run() throws Exception {
            if (proxies != null && proxies.size() > 0) {
                NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
                ProxyService proxyService = nodeEngine.getProxyService();
                for (ProxyInfo proxy : proxies) {
                    proxyService.getDistributedObject(proxy.serviceName, proxy.objectId);
                }
            }
        }

        @Override
        public boolean returnsResponse(Throwable throwable) {
            return false;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            int len = proxies != null ? proxies.size() : 0;
            out.writeInt(len);
            if (len > 0) {
                for (ProxyInfo proxy : proxies) {
                    out.writeUTF(proxy.serviceName);
                    out.writeObject(proxy.objectId);
                }
            }
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            int len = in.readInt();
            if (len > 0) {
                proxies = new ArrayList<ProxyInfo>(len);
                for (int i = 0; i < len; i++) {
                    ProxyInfo proxy = new ProxyInfo(in.readUTF(), in.readObject());
                    proxies.add(proxy);
                }
            }
        }
    }

    private static class ProxyInfo {
        final String serviceName;
        final Object objectId;

        private ProxyInfo(String serviceName, Object objectId) {
            this.serviceName = serviceName;
            this.objectId = objectId;
        }
    }

    void shutdown() {
        for (ProxyRegistry registry : registries.values()) {
            registry.destroy();
        }
        registries.clear();
        listeners.clear();
    }
}
