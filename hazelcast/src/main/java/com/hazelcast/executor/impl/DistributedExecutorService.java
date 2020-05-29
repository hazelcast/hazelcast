/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.executor.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.executor.LocalExecutorStats;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_PREFIX;
import static com.hazelcast.internal.metrics.impl.ProviderHelper.provide;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;

public class DistributedExecutorService implements ManagedService, RemoteService,
                                                   StatisticsAwareService<LocalExecutorStats>, SplitBrainProtectionAwareService,
                                                   DynamicMetricsProvider {

    public static final String SERVICE_NAME = "hz:impl:executorService";

    private static final Object NULL_OBJECT = new Object();

    // Updates the CallableProcessor.responseFlag field. An AtomicBoolean is simpler, but creates another unwanted
    // object. Using this approach, you don't create that object.
    private static final AtomicReferenceFieldUpdater<Processor, Boolean> RESPONSE_FLAG =
            AtomicReferenceFieldUpdater.newUpdater(Processor.class, Boolean.class, "responseFlag");

    // package-local access to allow test to inspect the map's values
    final ConcurrentMap<String, ExecutorConfig> executorConfigCache = new ConcurrentHashMap<String, ExecutorConfig>();

    private NodeEngine nodeEngine;
    private ExecutionService executionService;
    private final ConcurrentMap<UUID, Processor> submittedTasks = new ConcurrentHashMap<>();
    private final Set<String> shutdownExecutors
            = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ConcurrentHashMap<String, LocalExecutorStatsImpl> statsMap = new ConcurrentHashMap<>();
    private final ConstructorFunction<String, LocalExecutorStatsImpl> localExecutorStatsConstructorFunction
            = key -> new LocalExecutorStatsImpl();

    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            ExecutorConfig executorConfig = nodeEngine.getConfig().findExecutorConfig(name);
            String splitBrainProtectionName = executorConfig.getSplitBrainProtectionName();
            return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
        }
    };

    private ILogger logger;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.executionService = nodeEngine.getExecutionService();
        this.logger = nodeEngine.getLogger(DistributedExecutorService.class);

        ((NodeEngineImpl) nodeEngine).getMetricsRegistry().registerDynamicMetricsProvider(this);
    }

    @Override
    public void reset() {
        shutdownExecutors.clear();
        submittedTasks.clear();
        statsMap.clear();
        executorConfigCache.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    public <T> void execute(String name, UUID uuid,
                            @Nonnull T task, Operation op) {
        ExecutorConfig cfg = getOrFindExecutorConfig(name);
        if (cfg.isStatisticsEnabled()) {
            startPending(name);
        }
        Processor processor;
        if (task instanceof Runnable) {
            processor = new Processor(name, uuid, (Runnable) task, op, cfg.isStatisticsEnabled());
        } else {
            processor = new Processor(name, uuid, (Callable) task, op, cfg.isStatisticsEnabled());
        }
        if (uuid != null) {
            submittedTasks.put(uuid, processor);
        }

        try {
            executionService.execute(name, processor);
        } catch (RejectedExecutionException e) {
            if (cfg.isStatisticsEnabled()) {
                rejectExecution(name);
            }
            logger.warning("While executing " + task + " on Executor[" + name + "]", e);
            if (uuid != null) {
                submittedTasks.remove(uuid);
            }
            processor.sendResponse(e);
        }
    }

    public boolean cancel(UUID uuid, boolean interrupt) {
        Processor processor = submittedTasks.remove(uuid);
        if (processor != null && processor.cancel(interrupt)) {
            if (processor.sendResponse(new CancellationException())) {
                if (processor.isStatisticsEnabled()) {
                    getLocalExecutorStats(processor.name).cancelExecution();
                }
                return true;
            }
        }
        return false;
    }

    public String getName(UUID uuid) {
        Processor proc = submittedTasks.get(uuid);
        if (proc != null) {
            return proc.name;
        }
        return null;
    }

    public void shutdownExecutor(String name) {
        executionService.shutdownExecutor(name);
        shutdownExecutors.add(name);
        executorConfigCache.remove(name);
    }

    public boolean isShutdown(String name) {
        return shutdownExecutors.contains(name);
    }

    @Override
    public ExecutorServiceProxy createDistributedObject(String name, UUID source, boolean local) {
        return new ExecutorServiceProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name, boolean local) {
        shutdownExecutors.remove(name);
        executionService.shutdownExecutor(name);
        statsMap.remove(name);
        executorConfigCache.remove(name);
        splitBrainProtectionConfigCache.remove(name);
    }

    LocalExecutorStatsImpl getLocalExecutorStats(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, localExecutorStatsConstructorFunction);
    }

    private void startExecution(String name, long elapsed) {
        getLocalExecutorStats(name).startExecution(elapsed);
    }

    private void finishExecution(String name, long elapsed) {
        getLocalExecutorStats(name).finishExecution(elapsed);
    }

    private void startPending(String name) {
        getLocalExecutorStats(name).startPending();
    }

    private void rejectExecution(String name) {
        getLocalExecutorStats(name).rejectExecution();
    }

    @Override
    public Map<String, LocalExecutorStats> getStats() {
        Map<String, LocalExecutorStats> executorStats = MapUtil.createHashMap(statsMap.size());
        Config config = nodeEngine.getConfig();
        for (Map.Entry<String, LocalExecutorStatsImpl> executorStat : statsMap.entrySet()) {
            String name = executorStat.getKey();
            if (config.getExecutorConfig(name).isStatisticsEnabled()) {
                executorStats.put(name, executorStat.getValue());
            }
        }
        return executorStats;
    }

    /**
     * Locate the {@code ExecutorConfig} in local {@link #executorConfigCache} or find it from {@link NodeEngine#getConfig()} and
     * cache it locally.
     *
     * @param name
     * @return
     */
    private ExecutorConfig getOrFindExecutorConfig(String name) {
        ExecutorConfig cfg = executorConfigCache.get(name);
        if (cfg != null) {
            return cfg;
        } else {
            cfg = nodeEngine.getConfig().findExecutorConfig(name);
            ExecutorConfig executorConfig = executorConfigCache.putIfAbsent(name, cfg);
            return executorConfig == null ? cfg : executorConfig;
        }
    }

    @Override
    public String getSplitBrainProtectionName(final String name) {
        if (name == null) {
            // see CancellationOperation#getName()
            return null;
        }
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        provide(descriptor, context, EXECUTOR_PREFIX, getStats());
    }

    private final class Processor extends FutureTask implements Runnable {
        //is being used through the RESPONSE_FLAG. Can't be private due to reflection constraint.
        volatile Boolean responseFlag = Boolean.FALSE;

        private final String name;
        private final UUID uuid;
        private final Operation op;
        private final String taskToString;
        private final long creationTime = Clock.currentTimeMillis();
        private final boolean statisticsEnabled;

        private Processor(String name, UUID uuid,
                          @Nonnull Callable callable,
                          Operation op,
                          boolean statisticsEnabled) {
            //noinspection unchecked
            super(callable);
            this.name = name;
            this.uuid = uuid;
            this.taskToString = String.valueOf(callable);
            this.op = op;
            this.statisticsEnabled = statisticsEnabled;
        }

        private Processor(String name, UUID uuid,
                          @Nonnull Runnable runnable,
                          Operation op, boolean statisticsEnabled) {
            //noinspection unchecked
            super(runnable, null);
            this.name = name;
            this.uuid = uuid;
            this.taskToString = String.valueOf(runnable);
            this.op = op;
            this.statisticsEnabled = statisticsEnabled;
        }

        @Override
        public void run() {
            long start = Clock.currentTimeMillis();
            if (statisticsEnabled) {
                startExecution(name, start - creationTime);
            }
            Object result = null;
            try {
                super.run();
                if (!isCancelled()) {
                    result = get();
                }
            } catch (Exception e) {
                logException(e);
                result = e;
            } finally {
                if (uuid != null) {
                    submittedTasks.remove(uuid);
                }
                if (!isCancelled()) {
                    sendResponse(result);
                    if (statisticsEnabled) {
                        finishExecution(name, Clock.currentTimeMillis() - start);
                    }
                }
            }
        }

        private void logException(Exception e) {
            if (logger.isFinestEnabled()) {
                logger.finest("While executing callable: " + taskToString, e);
            }
        }

        private boolean sendResponse(Object result) {
            if (RESPONSE_FLAG.compareAndSet(this, Boolean.FALSE, Boolean.TRUE)) {
                try {
                    op.sendResponse(result);
                } catch (HazelcastSerializationException e) {
                    op.sendResponse(e);
                }
                return true;
            }

            return false;
        }

        boolean isStatisticsEnabled() {
            return statisticsEnabled;
        }
    }

}
