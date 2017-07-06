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

package com.hazelcast.internal.util;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.internal.util.futures.SerialInvokeOnAllMemberFuture;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.internal.util.futures.SerialInvokeOnAllMemberFuture.IGNORE_CLUSTER_TOPOLOGY_CHANGES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Utility methods for invocations
 */
public final class InvocationUtil {
    private static final int WARMUP_SLEEPING_TIME_MILLIS = 10;
    private static final int INVOCATION_TIMEOUT_SECONDS = 60;

    private InvocationUtil() {

    }

    /**
     * Invoke operation on all clusters members.
     *
     * The invocation is serial: It iterates over all members starting from the oldest member to the youngest one.
     * If there is a cluster membership change while invoking then it will restart invocations on all members. This
     * implies the operation should be idempotent.
     *
     * If there is an exception - other than {@link MemberLeftException} while invoking then the iteration
     * is interrupted and the exception is propagates to the caller.
     *
     * @param nodeEngine
     * @param operationFactory
     * @param retriesCount
     */
    public static void invokeOnStableClusterSerial(NodeEngine nodeEngine, OperationFactory operationFactory,
                                                   int retriesCount) {
        warmUpPartitions(nodeEngine);
        SerialInvokeOnAllMemberFuture<Object> future = new SerialInvokeOnAllMemberFuture<Object>(operationFactory, nodeEngine,
                IGNORE_CLUSTER_TOPOLOGY_CHANGES, retriesCount);
        try {
            future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    private static boolean invokeOnMembers(OperationService operationService, OperationFactory operationFactory,
                                        Collection<Member> members) {
        for (Member member : members) {
            Address target = member.getAddress();
            Operation operation = operationFactory.createOperation();
            String serviceName = operation.getServiceName();
            InternalCompletableFuture<?> future = operationService.invokeOnTarget(serviceName, operation, target);
            try {
                future.get(INVOCATION_TIMEOUT_SECONDS, SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new HazelcastException("Interrupted while invoking " + operation + " on " + member, e);
            } catch (MemberLeftException e) {
                return false;
            } catch (ExecutionException e) {
                throw new HazelcastException("Error while invoking " + operation + " on " + member, e);
            } catch (TimeoutException e) {
                throw new HazelcastException("Timeout while invoking " + operation + " on " + member, e);
            }
        }
        return true;
    }

    private static void warmUpPartitions(NodeEngine nodeEngine) {
        final PartitionService ps = nodeEngine.getHazelcastInstance().getPartitionService();
        for (Partition partition : ps.getPartitions()) {
            while (partition.getOwner() == null) {
                try {
                    Thread.sleep(WARMUP_SLEEPING_TIME_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new HazelcastException("Thread interrupted while initializing a partition table", e);
                }
            }
        }
    }
}
