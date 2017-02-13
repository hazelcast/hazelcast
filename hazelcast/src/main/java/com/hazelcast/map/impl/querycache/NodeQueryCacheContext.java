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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.core.IFunction;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.instance.LifecycleServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;

/**
 * Node side implementation of {@link QueryCacheContext}.
 *
 * @see QueryCacheContext
 */
public class NodeQueryCacheContext implements QueryCacheContext {

    private final NodeEngine nodeEngine;
    private final MapServiceContext mapServiceContext;
    private final QueryCacheEventService queryCacheEventService;
    private final QueryCacheConfigurator queryCacheConfigurator;
    private final QueryCacheScheduler queryCacheScheduler;
    private final InvokerWrapper invokerWrapper;

    // these fields are not final for testing purposes
    private PublisherContext publisherContext;
    private SubscriberContext subscriberContext;

    public NodeQueryCacheContext(MapServiceContext mapServiceContext) {
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.mapServiceContext = mapServiceContext;
        this.queryCacheScheduler = new NodeQueryCacheScheduler(mapServiceContext);
        this.queryCacheEventService = new NodeQueryCacheEventService(mapServiceContext);
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
     * {@link com.hazelcast.spi.EventService} can drop them.
     */
    private void flushPublishersOnNodeShutdown() {
        Node node = ((NodeEngineImpl) this.nodeEngine).getNode();
        LifecycleServiceImpl lifecycleService = node.hazelcastInstance.getLifecycleService();
        lifecycleService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (SHUTTING_DOWN == event.getState()) {
                    publisherContext.flush();
                }
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
        List<Member> members = new ArrayList<Member>(memberList.size());
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
    public InvokerWrapper getInvokerWrapper() {
        return invokerWrapper;
    }

    @Override
    public Object toObject(Object obj) {
        return mapServiceContext.toObject(obj);
    }

    private String registerLocalIMapListener(String mapName) {
        return mapServiceContext.addLocalListenerAdapter(new ListenerAdapter<IMapEvent>() {
            @Override
            public void onEvent(IMapEvent event) {
                // NOP
            }
        }, mapName);
    }

    @SerializableByConvention
    private class RegisterMapListenerFunction implements IFunction<String, String> {
        @Override
        public String apply(String mapName) {
            return registerLocalIMapListener(mapName);
        }
    };
}
