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
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class DistributedExecutorService implements ManagedService, RemoteService {

    public static final String SERVICE_NAME = "hz:impl:executorService";

    //Updates the CallableProcessor.responseFlag field. An AtomicBoolean is simpler, but creates another unwanted
    //object. Using this approach, you don't create that object.
    private static final AtomicReferenceFieldUpdater<CallableProcessor, Boolean> responseFlagFieldUpdater =
            AtomicReferenceFieldUpdater.newUpdater(CallableProcessor.class, Boolean.class, "responseFlag");

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

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.executionService = nodeEngine.getExecutionService();
    }

    @Override
    public void reset() {
        shutdownExecutors.clear();
        submittedTasks.clear();
        statsMap.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    public void execute(String name, String uuid, Callable callable, ResponseHandler responseHandler) {
        startPending(name);
        CallableProcessor processor = new CallableProcessor(name, uuid, callable, responseHandler);
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
        CallableProcessor processor = submittedTasks.remove(uuid);
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

    @Override
    public ExecutorServiceProxy createDistributedObject(String name) {
        return new ExecutorServiceProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
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

        private final String name;
        private final String uuid;
        private final ResponseHandler responseHandler;
        private final String callableToString;
        private final long creationTime = Clock.currentTimeMillis();
        //is being used through the responseFlagFieldUpdater. Can't be private due to reflection constraint.
        volatile Boolean responseFlag = Boolean.FALSE;

        private CallableProcessor(String name, String uuid, Callable callable, ResponseHandler responseHandler) {
            //noinspection unchecked
            super(callable);
            this.name = name;
            this.uuid = uuid;
            this.callableToString = String.valueOf(callable);
            this.responseHandler = responseHandler;
        }

        @Override
        public void run() {
            long start = Clock.currentTimeMillis();
            startExecution(name, start - creationTime);
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
                sendResponse(result);
                if (!isCancelled()) {
                    finishExecution(name, Clock.currentTimeMillis() - start);
                }
            }
        }

        private void logException(Exception e) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("While executing callable: " + callableToString, e);
            }
        }

        private void sendResponse(Object result) {
            if (responseFlagFieldUpdater.compareAndSet(this, Boolean.FALSE, Boolean.TRUE)) {
                responseHandler.sendResponse(result);
            }
        }
    }

    private ILogger getLogger() {
        return nodeEngine.getLogger(DistributedExecutorService.class);
    }
}
