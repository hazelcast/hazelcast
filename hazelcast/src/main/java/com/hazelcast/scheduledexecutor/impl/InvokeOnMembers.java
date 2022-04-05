/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.internal.serialization.SerializationService;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * Executes an operation on a set of targets. Similar to {@link com.hazelcast.spi.impl.operationservice.impl.InvokeOnPartitions}
 * but for members.
 */
@PrivateApi
public final class InvokeOnMembers {

    private static final int TRY_COUNT = 10;
    private static final int TRY_PAUSE_MILLIS = 300;

    private final ILogger logger;

    private final OperationService operationService;
    private final SerializationService serializationService;

    private final String serviceName;
    private final Supplier<Operation> operationFactory;
    private final Collection<Member> targets;
    private final Map<Member, Future> futures;
    private final Map<Member, Object> results;

    public InvokeOnMembers(NodeEngine nodeEngine, String serviceName, Supplier<Operation> operationFactory,
                           Collection<Member> targets) {
        this.logger = nodeEngine.getLogger(getClass());
        this.operationService = nodeEngine.getOperationService();
        this.serializationService = nodeEngine.getSerializationService();
        this.serviceName = serviceName;
        this.operationFactory = operationFactory;
        this.targets = targets;
        this.futures = new HashMap<Member, Future>(targets.size());
        this.results = new HashMap<Member, Object>(targets.size());
    }

    /**
     * Executes the operation on all targets.
     */
    public Map<Member, Object> invoke()
            throws Exception {
        invokeOnAllTargets();

        awaitCompletion();

        retryFailedTargets();

        return results;
    }

    private void invokeOnAllTargets() {
        for (Member target : targets) {
            Future future = operationService.createInvocationBuilder(serviceName, operationFactory.get(), target.getAddress())
                                            .setTryCount(TRY_COUNT).setTryPauseMillis(TRY_PAUSE_MILLIS).invoke();
            futures.put(target, future);
        }
    }

    private void awaitCompletion() {
        for (Map.Entry<Member, Future> responseEntry : futures.entrySet()) {
            try {
                Future future = responseEntry.getValue();
                results.put(responseEntry.getKey(), serializationService.toObject(future.get()));
            } catch (Throwable t) {
                if (logger.isFinestEnabled()) {
                    logger.finest(t);
                } else {
                    logger.warning(t.getMessage());
                }

                results.put(responseEntry.getKey(), t);
            }
        }
    }

    private void retryFailedTargets()
            throws InterruptedException, ExecutionException {
        List<Member> failedMembers = new LinkedList<Member>();
        for (Map.Entry<Member, Object> memberResult : results.entrySet()) {
            Member member = memberResult.getKey();
            Object result = memberResult.getValue();
            if (result instanceof Throwable) {
                failedMembers.add(member);
            }
        }

        for (Member failedMember : failedMembers) {
            Operation operation = operationFactory.get();
            Future future = operationService.createInvocationBuilder(serviceName, operation, failedMember.getAddress()).invoke();
            results.put(failedMember, future);
        }

        for (Member failedMember : failedMembers) {
            Future future = (Future) results.get(failedMember);
            Object result = future.get();
            results.put(failedMember, result);
        }
    }
}
