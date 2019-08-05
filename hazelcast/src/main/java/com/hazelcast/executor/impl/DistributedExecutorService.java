/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.QuorumAwareService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;
import com.hazelcast.util.MapUtil;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

public class DistributedExecutorService implements ManagedService, RemoteService,
        StatisticsAwareService<LocalExecutorStats>, QuorumAwareService {

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
    private final ConcurrentMap<String, Processor> submittedTasks = new ConcurrentHashMap<>(100);
    private final Set<String> shutdownExecutors
            = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ConcurrentHashMap<String, LocalExecutorStatsImpl> statsMap = new ConcurrentHashMap<>();
    private final ConstructorFunction<String, LocalExecutorStatsImpl> localExecutorStatsConstructorFunction
            = key -> new LocalExecutorStatsImpl();

    private final ConcurrentMap<String, Object> quorumConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory quorumConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> quorumConfigConstructor = new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            ExecutorConfig executorConfig = nodeEngine.getConfig().findExecutorConfig(name);
            String quorumName = executorConfig.getQuorumName();
            return quorumName == null ? NULL_OBJECT : quorumName;
        }
    };

    private ILogger logger;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.executionService = nodeEngine.getExecutionService();
        this.logger = nodeEngine.getLogger(DistributedExecutorService.class);
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

    public <T> void execute(String name, String uuid, T task, Operation op) {
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

    public boolean cancel(String uuid, boolean interrupt) {
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

    public String getName(String uuid) {
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
    public ExecutorServiceProxy createDistributedObject(String name) {
        return new ExecutorServiceProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
        shutdownExecutors.remove(name);
        executionService.shutdownExecutor(name);
        statsMap.remove(name);
        executorConfigCache.remove(name);
        quorumConfigCache.remove(name);
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
        for (Map.Entry<String, LocalExecutorStatsImpl> queueStat : statsMap.entrySet()) {
            executorStats.put(queueStat.getKey(), queueStat.getValue());
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
    public String getQuorumName(final String name) {
        if (name == null) {
            // see CancellationOperation#getName()
            return null;
        }
        Object quorumName = getOrPutSynchronized(quorumConfigCache, name, quorumConfigCacheMutexFactory,
                quorumConfigConstructor);
        return quorumName == NULL_OBJECT ? null : (String) quorumName;
    }

    private final class Processor extends FutureTask implements Runnable {
        //is being used through the RESPONSE_FLAG. Can't be private due to reflection constraint.
        volatile Boolean responseFlag = Boolean.FALSE;

        private final String name;
        private final String uuid;
        private final Operation op;
        private final String taskToString;
        private final long creationTime = Clock.currentTimeMillis();
        private final boolean statisticsEnabled;

        private Processor(String name, String uuid, Callable callable, Operation op, boolean statisticsEnabled) {
            //noinspection unchecked
            super(callable);
            this.name = name;
            this.uuid = uuid;
            this.taskToString = String.valueOf(callable);
            this.op = op;
            this.statisticsEnabled = statisticsEnabled;
        }

        private Processor(String name, String uuid, Runnable runnable, Operation op, boolean statisticsEnabled) {
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
