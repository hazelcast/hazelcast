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

package com.hazelcast.executor.impl;

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
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

public class DistributedExecutorService implements ManagedService, RemoteService, LiveOperationsTracker,
        StatisticsAwareService {

    public static final String SERVICE_NAME = "hz:impl:executorService";

    // Updates the CallableProcessor.responseFlag field. An AtomicBoolean is simpler, but creates another unwanted
    // object. Using this approach, you don't create that object.
    private static final AtomicReferenceFieldUpdater<CallableProcessor, Boolean> RESPONSE_FLAG =
            AtomicReferenceFieldUpdater.newUpdater(CallableProcessor.class, Boolean.class, "responseFlag");

    // package-local access to allow test to inspect the map's values
    final ConcurrentMap<String, ExecutorConfig> executorConfigCache = new ConcurrentHashMap<String, ExecutorConfig>();

    private NodeEngine nodeEngine;
    private ExecutionService executionService;
    private final ConcurrentMap<String, CallableProcessor> submittedTasks
            = new ConcurrentHashMap<String, CallableProcessor>(100);
    private final Set<String> shutdownExecutors
            = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private final ConcurrentHashMap<String, LocalExecutorStatsImpl> statsMap
            = new ConcurrentHashMap<String, LocalExecutorStatsImpl>();
    private final ConstructorFunction<String, LocalExecutorStatsImpl> localExecutorStatsConstructorFunction
            = new ConstructorFunction<String, LocalExecutorStatsImpl>() {
        public LocalExecutorStatsImpl createNew(String key) {
            return new LocalExecutorStatsImpl();
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

    public void execute(String name, String uuid, Callable callable, Operation op) {
        ExecutorConfig cfg = getOrFindExecutorConfig(name);
        if (cfg.isStatisticsEnabled()) {
            startPending(name);
        }
        CallableProcessor processor = new CallableProcessor(name, uuid, callable, op, cfg.isStatisticsEnabled());
        if (uuid != null) {
            submittedTasks.put(uuid, processor);
        }

        try {
            executionService.execute(name, processor);
        } catch (RejectedExecutionException e) {
            if (cfg.isStatisticsEnabled()) {
                rejectExecution(name);
            }
            logger.warning("While executing " + callable + " on Executor[" + name + "]", e);
            if (uuid != null) {
                submittedTasks.remove(uuid);
            }
            processor.sendResponse(e);
        }
    }

    public boolean cancel(String uuid, boolean interrupt) {
        CallableProcessor processor = submittedTasks.remove(uuid);
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
    public void populate(LiveOperations result) {
        for (CallableProcessor processor : submittedTasks.values()) {
            Operation op = processor.op;
            result.add(op.getCallerAddress(), op.getCallId());
        }
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

    private final class CallableProcessor extends FutureTask implements Runnable {
        //is being used through the RESPONSE_FLAG. Can't be private due to reflection constraint.
        volatile Boolean responseFlag = Boolean.FALSE;

        private final String name;
        private final String uuid;
        private final Operation op;
        private final String callableToString;
        private final long creationTime = Clock.currentTimeMillis();
        private final boolean statisticsEnabled;

        private CallableProcessor(String name, String uuid, Callable callable, Operation op, boolean statisticsEnabled) {
            //noinspection unchecked
            super(callable);
            this.name = name;
            this.uuid = uuid;
            this.callableToString = String.valueOf(callable);
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
                logger.finest("While executing callable: " + callableToString, e);
            }
        }

        private boolean sendResponse(Object result) {
            if (RESPONSE_FLAG.compareAndSet(this, Boolean.FALSE, Boolean.TRUE)) {
                op.sendResponse(result);
                return true;
            }

            return false;
        }

        boolean isStatisticsEnabled() {
            return statisticsEnabled;
        }
    }
}
