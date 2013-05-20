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

import com.hazelcast.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.spi.*;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * @mdogan 1/18/13
 */
public class DistributedExecutorService implements ManagedService, RemoteService {

    public static final String SERVICE_NAME = "hz:impl:executorService";

    private final Set<String> shutdownExecutors = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private final ConcurrentHashMap<String, LocalExecutorStatsImpl> statsMap = new ConcurrentHashMap<String, LocalExecutorStatsImpl>();
    private final ConstructorFunction<String, LocalExecutorStatsImpl> localExecutorStatsConstructorFunction = new ConstructorFunction<String, LocalExecutorStatsImpl>() {
        public LocalExecutorStatsImpl createNew(String key) {
            return new LocalExecutorStatsImpl();
        }
    };
    private NodeEngine nodeEngine;
    private ExecutionService executionService;

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.executionService = nodeEngine.getExecutionService();
    }

    public void reset() {
        shutdownExecutors.clear();
    }

    public void shutdown() {
        reset();
    }

    public void execute(String name, final Callable callable, final ResponseHandler responseHandler) {
        startPending(name);
        executionService.execute(name, new CallableProcessor(name, callable, responseHandler));
    }

    public void shutdownExecutor(String name) {
        executionService.shutdownExecutor(name);
        shutdownExecutors.add(name);
    }

    public boolean isShutdown(String name) {
        return shutdownExecutors.contains(name);
    }

    public String getServiceName() {
        return SERVICE_NAME;
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

    private class CallableProcessor implements Runnable {
        final Callable callable;
        final ResponseHandler responseHandler;
        final long creationTime = Clock.currentTimeMillis();
        final String name;

        private CallableProcessor(String name, Callable callable, ResponseHandler responseHandler) {
            this.name = name;
            this.callable = callable;
            this.responseHandler = responseHandler;
        }

        public void run() {
            final long start = Clock.currentTimeMillis();
            startExecution(name, start - creationTime);
            Object result = null;
            try {
                result = callable.call();
            } catch (Exception e) {
                nodeEngine.getLogger(DistributedExecutorService.class.getName())
                        .log(Level.FINEST, "While executing callable: " + callable, e);
                result = e;
            } finally {
                responseHandler.sendResponse(result);
                finishExecution(name, Clock.currentTimeMillis() - start);
            }
        }
    }

}
