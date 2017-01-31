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

package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.core.IFunction;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheScheduler;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorScannerTask;
import com.hazelcast.map.impl.querycache.accumulator.DefaultAccumulatorInfoSupplier;
import com.hazelcast.spi.NodeEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.querycache.publisher.AccumulatorSweeper.flushAllAccumulators;
import static com.hazelcast.util.CollectionUtil.isEmpty;

/**
 * Default implementation of {@link PublisherContext}.
 *
 * @see PublisherContext
 */
public class DefaultPublisherContext implements PublisherContext {

    private static final long SCAN_PERIOD_SECONDS = 5L;
    private static final long ORPHANED_QUERY_CACHE_REMOVAL_DELAY_SECONDS = TimeUnit.MINUTES.toSeconds(10);

    private final QueryCacheContext context;
    private final NodeEngine nodeEngine;
    private final MapListenerRegistry mapListenerRegistry;
    private final MapPublisherRegistry mapPublisherRegistry;
    private final AccumulatorInfoSupplier accumulatorInfoSupplier;
    private final IFunction<String, String> listenerRegistrator;
    private final ConcurrentMap<String, ScheduledFuture> removalCandidateFutures;

    public DefaultPublisherContext(QueryCacheContext context, NodeEngine nodeEngine,
                                   IFunction<String, String> listenerRegistrator) {
        this.context = context;
        this.nodeEngine = nodeEngine;
        this.mapListenerRegistry = new MapListenerRegistry(context);
        this.mapPublisherRegistry = new MapPublisherRegistry(context);
        this.accumulatorInfoSupplier = new DefaultAccumulatorInfoSupplier();
        this.listenerRegistrator = listenerRegistrator;
        this.removalCandidateFutures = new ConcurrentHashMap<String, ScheduledFuture>();

        startBackgroundAccumulatorScanner();
        handleSubscriberAddRemove();
    }

    @Override
    public AccumulatorInfoSupplier getAccumulatorInfoSupplier() {
        return accumulatorInfoSupplier;
    }

    @Override
    public MapPublisherRegistry getMapPublisherRegistry() {
        return mapPublisherRegistry;
    }

    @Override
    public MapListenerRegistry getMapListenerRegistry() {
        return mapListenerRegistry;
    }

    @Override
    public QueryCacheContext getContext() {
        return context;
    }

    @Override
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public IFunction<String, String> getListenerRegistrator() {
        return listenerRegistrator;
    }

    @Override
    public void handleDisconnectedSubscriber(String uuid) {
        Collection<PartitionAccumulatorRegistry> removalCandidates = getRemovalCandidates(uuid);
        if (isEmpty(removalCandidates)) {
            return;
        }
        startRemovalTask(removalCandidates, uuid);
    }

    // TODO handling client reconnection seems not straightforward with ClientAwareService
    @Override
    public void handleConnectedSubscriber(String uuid) {
        cancelRemovalTask(uuid);
    }

    @Override
    public void flush() {
        flushAllAccumulators(this);
    }

    private Collection<PartitionAccumulatorRegistry> getRemovalCandidates(String uuid) {
        List<PartitionAccumulatorRegistry> candidates = new ArrayList<PartitionAccumulatorRegistry>();
        MapPublisherRegistry mapPublisherRegistry = getMapPublisherRegistry();
        Map<String, PublisherRegistry> all = mapPublisherRegistry.getAll();
        for (PublisherRegistry publisherRegistry : all.values()) {
            final Map<String, PartitionAccumulatorRegistry> partitionAccumulators = publisherRegistry.getAll();
            Set<Map.Entry<String, PartitionAccumulatorRegistry>> entries = partitionAccumulators.entrySet();
            for (Map.Entry<String, PartitionAccumulatorRegistry> entry : entries) {
                PartitionAccumulatorRegistry accumulatorRegistry = entry.getValue();
                if (uuid.equals(accumulatorRegistry.getUuid())) {
                    candidates.add(accumulatorRegistry);
                }
            }
        }
        return candidates;
    }

    private PartitionAccumulatorRegistry removePartitionAccumulatorRegistry(PartitionAccumulatorRegistry registry) {
        AccumulatorInfo info = registry.getInfo();
        String mapName = info.getMapName();
        String cacheName = info.getCacheName();

        MapPublisherRegistry mapPublisherRegistry = getMapPublisherRegistry();
        PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrNull(mapName);
        if (publisherRegistry == null) {
            return null;
        }

        return publisherRegistry.remove(cacheName);
    }

    private void startRemovalTask(final Collection<PartitionAccumulatorRegistry> removalCandidates, String uuid) {
        QueryCacheScheduler queryCacheScheduler = context.getQueryCacheScheduler();
        ScheduledFuture scheduledFuture = queryCacheScheduler.scheduleWithRepetition(new Runnable() {
            @Override
            public void run() {
                for (PartitionAccumulatorRegistry registry : removalCandidates) {
                    removePartitionAccumulatorRegistry(registry);
                }
            }
        }, ORPHANED_QUERY_CACHE_REMOVAL_DELAY_SECONDS);

        ScheduledFuture prevFuture = removalCandidateFutures.put(uuid, scheduledFuture);
        if (prevFuture != null) {
            prevFuture.cancel(false);
        }
    }

    private void cancelRemovalTask(String uuid) {
        removalCandidateFutures.remove(uuid);
    }

    private void startBackgroundAccumulatorScanner() {
        QueryCacheScheduler scheduler = context.getQueryCacheScheduler();
        scheduler.scheduleWithRepetition(new AccumulatorScannerTask(context), SCAN_PERIOD_SECONDS);
    }

    private void handleSubscriberAddRemove() {
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        clusterService.addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                Member member = membershipEvent.getMember();
                String uuid = member.getUuid();
                handleDisconnectedSubscriber(uuid);
            }

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                Member member = membershipEvent.getMember();
                String uuid = member.getUuid();
                handleConnectedSubscriber(uuid);
            }
        });
    }
}
