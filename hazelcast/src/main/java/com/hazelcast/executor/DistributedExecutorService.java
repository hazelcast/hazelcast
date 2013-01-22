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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.ExecutionServiceImpl;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.logging.Level;

/**
 * @mdogan 1/18/13
 */
public class DistributedExecutorService implements ManagedService, RemoteService {

    public static final String SERVICE_NAME = "hz:impl:executorService";

    private NodeEngine nodeEngine;
    private ExecutionServiceImpl executionService;

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.executionService = (ExecutionServiceImpl) nodeEngine.getExecutionService();
    }

    public void execute(String name, final Callable callable, final ResponseHandler responseHandler) {
        executionService.execute(name, new Runnable() {
            public void run() {
                Object result = null;
                try {
                    result = callable.call();
                } catch (Exception e) {
                    nodeEngine.getLogger(DistributedExecutorService.class.getName())
                            .log(Level.FINEST, "While executing callable: " + callable, e);
                    result = e;
                } finally {
                    responseHandler.sendResponse(result);
                }
            }
        });
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public DistributedObject createDistributedObject(Object objectId) {
        return new ExecutorServiceProxy(String.valueOf(objectId), nodeEngine);
    }

    public DistributedObject createDistributedObjectForClient(Object objectId) {
        return null;
    }

    public void destroyDistributedObject(Object objectId) {
        executionService.destroyExecutor(String.valueOf(objectId));
    }

    public void destroy() {

    }
}
