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

package com.hazelcast.executor;

import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.spi.*;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mdogan 1/18/13
 */
public class DistributedExecutorService implements ManagedService, RemoteService {

    public static final String SERVICE_NAME = "hz:impl:executorService";

    private NodeEngine nodeEngine;
    private ExecutionService executionService;
    private final ConcurrentMap<String, CallableProcessor> submittedTasks = new ConcurrentHashMap<String, CallableProcessor>(100);
    private final Set<String> shutdownExecutors = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private final ConcurrentHashMap<String, LocalExecutorStatsImpl> statsMap = new ConcurrentHashMap<String, LocalExecutorStatsImpl>();
    private final ConstructorFunction<String, LocalExecutorStatsImpl> localExecutorStatsConstructorFunction = new ConstructorFunction<String, LocalExecutorStatsImpl>() {
        public LocalExecutorStatsImpl createNew(String key) {
            return new LocalExecutorStatsImpl();
        }
    };

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.executionService = nodeEngine.getExecutionService();
    }

    public void reset() {
        shutdownExecutors.clear();
        submittedTasks.clear();
        statsMap.clear();
    }

    public void shutdown() {
        reset();
    }

    public void execute(String name, String uuid, final Callable callable, final ResponseHandler responseHandler) {
        startPending(name);
        final CallableProcessor processor = new CallableProcessor(name, uuid, callable, responseHandler);
        if (uuid != null) {
            submittedTasks.put(uuid, processor);
        }
        try {
            executionService.execute(name, processor);
        } catch (RejectedExecutionException e) {
            getLogger().warning("While executing " + callable + " on Executor[" + name + "]", e);
            if (uuid != null) {
                submittedTasks.remove(uuid);
            }
            processor.sendResponse(e);
        }
    }

    public boolean cancel(String uuid, boolean interrupt) {
        final CallableProcessor processor = submittedTasks.remove(uuid);
        if (processor != null && processor.cancel(interrupt)) {
            processor.sendResponse(new CancellationException());
            getLocalExecutorStats(processor.name).cancelExecution();
            return true;
        }
        return false;
    }

    public void shutdownExecutor(String name) {
        executionService.shutdownExecutor(name);
        shutdownExecutors.add(name);
    }

    public boolean isShutdown(String name) {
        return shutdownExecutors.contains(name);
    }

    public ExecutorServiceProxy createDistributedObject(Object objectId) {
        final String name = String.valueOf(objectId);
        return new ExecutorServiceProxy(name, nodeEngine, this);
    }

    public void destroyDistributedObject(Object objectId) {
        final String name = String.valueOf(objectId);
        shutdownExecutors.remove(name);
        executionService.shutdownExecutor(name);
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

    private class CallableProcessor extends FutureTask implements Runnable {
        final String name;
        final String uuid;
        final ResponseHandler responseHandler;
        final String callableToString;
        final long creationTime = Clock.currentTimeMillis();
        final AtomicBoolean responseFlag = new AtomicBoolean(false);

        private CallableProcessor(String name, String uuid, Callable callable, ResponseHandler responseHandler) {
            super(callable);
            this.name = name;
            this.uuid = uuid;
            this.callableToString = String.valueOf(callable);
            this.responseHandler = responseHandler;
        }

        public void run() {
            final long start = Clock.currentTimeMillis();
            startExecution(name, start - creationTime);
            Object result = null;
            try {
                super.run();
                result = get();
            } catch (Exception e) {
                final ILogger logger = getLogger();
                logger.finest( "While executing callable: " + callableToString, e);
                result = e;
            } finally {
                if (uuid != null) {
                    submittedTasks.remove(uuid);
                }
                sendResponse(result);
                if (!isCancelled()) {
                    finishExecution(name, Clock.currentTimeMillis() - start);
                }
            }
        }

        private void sendResponse(Object result) {
            if (responseFlag.compareAndSet(false, true)) {
                responseHandler.sendResponse(result);
            }
        }
    }

    private ILogger getLogger() {
        return nodeEngine.getLogger(DistributedExecutorService.class.getName());
    }
}
