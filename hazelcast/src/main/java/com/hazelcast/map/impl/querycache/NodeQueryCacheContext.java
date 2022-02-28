/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.map.IMapEvent;
import com.hazelcast.instance.impl.LifecycleServiceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.querycache.publisher.DefaultPublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.subscriber.NodeQueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.subscriber.NodeQueryCacheEventService;
import com.hazelcast.map.impl.querycache.subscriber.NodeQueryCacheScheduler;
import com.hazelcast.map.impl.querycache.subscriber.NodeSubscriberContext;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.internal.util.ContextMutexFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;

/**
 * Node side implementation of {@link QueryCacheContext}.
 *
 * @see QueryCacheContext
 */
public class NodeQueryCacheContext implements QueryCacheContext {

    private final NodeEngine nodeEngine;
    private final InvokerWrapper invokerWrapper;
    private final MapServiceContext mapServiceContext;
    private final QueryCacheScheduler queryCacheScheduler;
    private final QueryCacheEventService queryCacheEventService;
    private final QueryCacheConfigurator queryCacheConfigurator;
    private final ContextMutexFactory lifecycleMutexFactory = new ContextMutexFactory();

    // these fields are not final for testing purposes
    private PublisherContext publisherContext;
    private SubscriberContext subscriberContext;

    public NodeQueryCacheContext(MapServiceContext mapServiceContext) {
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.mapServiceContext = mapServiceContext;
        this.queryCacheScheduler = new NodeQueryCacheScheduler(mapServiceContext);
        this.queryCacheEventService = new NodeQueryCacheEventService(mapServiceContext, lifecycleMutexFactory);
        this.queryCacheConfigurator = new NodeQueryCacheConfigurator(nodeEngine.getConfig(),
                nodeEngine.getConfigClassLoader(), queryCacheEventService);
        this.invokerWrapper = new NodeInvokerWrapper(nodeEngine.getOperationService());
        // init these in the end
        this.subscriberContext = new NodeSubscriberContext(this);
        this.publisherContext = new DefaultPublisherContext(this, nodeEngine, new RegisterMapListenerFunction());
        flushPublishersOnNodeShutdown();
    }

    /**
     * This is a best effort approach; there is no guarantee that events in publishers internal buffers will be fired,
     * {@link EventService} can drop them.
     */
    private void flushPublishersOnNodeShutdown() {
        Node node = ((NodeEngineImpl) this.nodeEngine).getNode();
        LifecycleServiceImpl lifecycleService = node.hazelcastInstance.getLifecycleService();
        lifecycleService.addLifecycleListener(event -> {
            if (SHUTTING_DOWN == event.getState()) {
                publisherContext.flush();
            }
        });
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PublisherContext getPublisherContext() {
        return publisherContext;
    }

    @Override
    public SubscriberContext getSubscriberContext() {
        return subscriberContext;
    }

    @Override
    public void setSubscriberContext(SubscriberContext subscriberContext) {
        this.subscriberContext = subscriberContext;
    }

    @Override
    public QueryCacheEventService getQueryCacheEventService() {
        return queryCacheEventService;
    }

    @Override
    public QueryCacheConfigurator getQueryCacheConfigurator() {
        return queryCacheConfigurator;
    }

    @Override
    public QueryCacheScheduler getQueryCacheScheduler() {
        return queryCacheScheduler;
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return (InternalSerializationService) nodeEngine.getSerializationService();
    }

    @Override
    public Address getThisNodesAddress() {
        return nodeEngine.getThisAddress();
    }

    @Override
    public Collection<Member> getMemberList() {
        Collection<MemberImpl> memberList = nodeEngine.getClusterService().getMemberImpls();
        List<Member> members = new ArrayList<>(memberList.size());
        members.addAll(memberList);
        return members;
    }

    @Override
    public int getPartitionId(Object object) {
        assert object != null;

        if (object instanceof Data) {
            nodeEngine.getPartitionService().getPartitionId((Data) object);
        }
        return nodeEngine.getPartitionService().getPartitionId(object);
    }

    @Override
    public int getPartitionCount() {
        return nodeEngine.getPartitionService().getPartitionCount();
    }

    @Override
    public InvokerWrapper getInvokerWrapper() {
        return invokerWrapper;
    }

    @Override
    public Object toObject(Object obj) {
        return mapServiceContext.toObject(obj);
    }

    @Override
    public ContextMutexFactory getLifecycleMutexFactory() {
        return lifecycleMutexFactory;
    }

    private UUID registerLocalIMapListener(final String name) {
        return mapServiceContext.addLocalListenerAdapter(new ListenerAdapter<IMapEvent>() {
            @Override
            public void onEvent(IMapEvent event) {
                // NOP
            }

            @Override
            public String toString() {
                return "Local IMap listener for the map '" + name + "'";
            }
        }, name);
    }

    private class RegisterMapListenerFunction implements Function<String, UUID> {
        @Override
        public UUID apply(String name) {
            return registerLocalIMapListener(name);
        }
    }
}
