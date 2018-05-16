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

package com.hazelcast.crdt.pncounter;

import com.hazelcast.cluster.impl.VectorClock;
import com.hazelcast.config.Config;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.crdt.CRDTReplicationAwareService;
import com.hazelcast.crdt.CRDTReplicationContainer;
import com.hazelcast.crdt.MutationDisallowedException;
import com.hazelcast.internal.util.Memoizer;
import com.hazelcast.monitor.LocalPNCounterStats;
import com.hazelcast.monitor.impl.LocalPNCounterStatsImpl;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.UuidUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

/**
 * Service responsible for {@link PNCounter} proxies and replication operation.
 */
public class PNCounterService implements
        ManagedService,
        RemoteService,
        CRDTReplicationAwareService<PNCounterImpl>,
        QuorumAwareService,
        StatisticsAwareService<LocalPNCounterStats> {
    /** The name under which this service is registered */
    public static final String SERVICE_NAME = "hz:impl:PNCounterService";

    /** Map from counter name to counter implementations */
    private final ConcurrentMap<String, PNCounterImpl> counters = new ConcurrentHashMap<String, PNCounterImpl>();

    /** Constructor function for counter implementations */
    private final ConstructorFunction<String, PNCounterImpl> counterConstructorFn =
            new ConstructorFunction<String, PNCounterImpl>() {
                @Override
                public PNCounterImpl createNew(String name) {
                    if (isShuttingDown) {
                        throw new MutationDisallowedException("Cannot create a new PN counter named " + name
                                + " because this instance is shutting down!");
                    }
                    return new PNCounterImpl(UuidUtil.newUnsecureUuidString(), name);
                }
            };

    /** Cache for quorum config names */
    private final Memoizer<String, Object> quorumConfigCache = new Memoizer<String, Object>(
            new ConstructorFunction<String, Object>() {
                @Override
                public Object createNew(String name) {
                    final PNCounterConfig counterConfig = nodeEngine.getConfig().findPNCounterConfig(name);
                    final String quorumName = counterConfig.getQuorumName();
                    return quorumName == null ? Memoizer.NULL_OBJECT : quorumName;
                }
            });

    /** Map from PN counter name to counter statistics */
    private final ConcurrentMap<String, LocalPNCounterStatsImpl> statsMap
            = new ConcurrentHashMap<String, LocalPNCounterStatsImpl>();
    /** Unmodifiable statistics map to return from {@link #getStats()} */
    private Map unmodifiableStatsMap = Collections.unmodifiableMap(statsMap);

    /** Constructor function for PN counter statistics */
    private final ConstructorFunction<String, LocalPNCounterStatsImpl> statsConstructorFunction =
            new ConstructorFunction<String, LocalPNCounterStatsImpl>() {
                public LocalPNCounterStatsImpl createNew(String name) {
                    return new LocalPNCounterStatsImpl();
                }
            };

    /** Mutex for creating new PN counters and for marking the service as shutting down */
    private final Object newCounterCreationMutex = new Object();

    /**
     * Flag marking this service as shutting down. New PN counters must not be
     * added if this is {@code true}
     */
    private volatile boolean isShuttingDown;

    private NodeEngine nodeEngine;

    /**
     * Returns the counter with the given {@code name}.
     */
    public PNCounterImpl getCounter(String name) {
        return getOrPutSynchronized(counters, name, newCounterCreationMutex, counterConstructorFn);
    }

    /**
     * Returns {@code true} if the CRDT state for the PN counter with the
     * given {@code name} is present on this node.
     *
     * @param name the PN counter name
     */
    public boolean containsCounter(String name) {
        return counters.containsKey(name);
    }

    /** Returns the PN counter statistics for the counter with the given {@code name} */
    public LocalPNCounterStatsImpl getLocalPNCounterStats(String name) {
        return getOrPutSynchronized(statsMap, name, statsMap, statsConstructorFunction);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        counters.clear();
        statsMap.clear();
    }

    @Override
    public PNCounterProxy createDistributedObject(String objectName) {
        return new PNCounterProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        counters.remove(objectName);
        statsMap.remove(objectName);
        quorumConfigCache.remove(objectName);
    }

    @Override
    public CRDTReplicationContainer prepareReplicationOperation(
            Map<String, VectorClock> previouslyReplicatedVectorClocks, int targetIndex) {
        final HashMap<String, VectorClock> currentVectorClocks = new HashMap<String, VectorClock>();
        final HashMap<String, PNCounterImpl> counters = new HashMap<String, PNCounterImpl>();
        final Config config = nodeEngine.getConfig();

        for (Entry<String, PNCounterImpl> counterEntry : this.counters.entrySet()) {
            final String counterName = counterEntry.getKey();
            final PNCounterImpl counter = counterEntry.getValue();
            if (targetIndex >= config.findPNCounterConfig(counterName).getReplicaCount()) {
                continue;
            }
            final VectorClock counterCurrentVectorClock = counter.getCurrentVectorClock();
            final VectorClock counterPreviousVectorClock = previouslyReplicatedVectorClocks.get(counterName);

            if (counterPreviousVectorClock == null || counterCurrentVectorClock.isAfter(counterPreviousVectorClock)) {
                counters.put(counterName, counter);
            }
            currentVectorClocks.put(counterName, counterCurrentVectorClock);
        }

        return counters.isEmpty()
                ? null
                : new CRDTReplicationContainer(new PNCounterReplicationOperation(counters), currentVectorClocks);
    }

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void merge(String name, PNCounterImpl value) {
        getCounter(name).merge(value);
    }

    @Override
    public CRDTReplicationContainer prepareMigrationOperation(int maxConfiguredReplicaCount) {
        final HashMap<String, VectorClock> currentVectorClocks = new HashMap<String, VectorClock>();
        final HashMap<String, PNCounterImpl> counters = new HashMap<String, PNCounterImpl>();
        final Config config = nodeEngine.getConfig();

        for (Entry<String, PNCounterImpl> counterEntry : this.counters.entrySet()) {
            final String counterName = counterEntry.getKey();
            final PNCounterImpl counter = counterEntry.getValue();
            if (config.findPNCounterConfig(counterName).getReplicaCount() >= maxConfiguredReplicaCount) {
                continue;
            }
            counters.put(counterName, counter);
            currentVectorClocks.put(counterName, counter.getCurrentVectorClock());
        }

        return counters.isEmpty()
                ? null
                : new CRDTReplicationContainer(new PNCounterReplicationOperation(counters), currentVectorClocks);
    }

    @Override
    public boolean clearCRDTState(Map<String, VectorClock> vectorClocks) {
        boolean allCleared = true;
        for (Entry<String, VectorClock> vectorClockEntry : vectorClocks.entrySet()) {
            final String counterName = vectorClockEntry.getKey();
            final VectorClock vectorClock = vectorClockEntry.getValue();
            final PNCounterImpl counter = counters.get(counterName);
            if (counter == null) {
                continue;
            }
            if (counter.markMigrated(vectorClock)) {
                counters.remove(counterName);
            } else {
                allCleared = false;
            }
        }
        return allCleared;
    }

    @Override
    public void prepareToSafeShutdown() {
        synchronized (newCounterCreationMutex) {
            isShuttingDown = true;
        }
        for (PNCounterImpl counter : this.counters.values()) {
            counter.markMigrated();
        }
    }

    @Override
    public String getQuorumName(String name) {
        return (String) quorumConfigCache.getOrCalculate(name);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, LocalPNCounterStats> getStats() {
        return unmodifiableStatsMap;
    }
}
