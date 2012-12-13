/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.transaction.TransactionImpl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @mdogan 8/24/12
 */
public interface NodeService {

    void runOperation(Operation op) throws Exception;

//    Map<Integer, Object> invokeOnAllPartitions(String serviceName, Operation op) throws Exception;

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId);

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target);

    boolean send(Operation op, int partitionId, int replicaIndex);

    boolean send(Operation op, Address target);

    boolean send(Operation op, Connection connection);

    void takeBackups(String serviceName, Operation op, int partitionId, int offset, int backupCount, int timeoutSeconds)
            throws ExecutionException, TimeoutException, InterruptedException;

    Address getThisAddress();

    int getPartitionId(Data key);

    int getPartitionCount();

    PartitionInfo getPartitionInfo(int partitionId);

    Config getConfig();

    GroupProperties getGroupProperties();

    Cluster getCluster();

    ILogger getLogger(String name);

    void execute(Runnable command);

    Future<?> submit(Runnable task);

    void schedule(Runnable command, long delay, TimeUnit unit);

    void scheduleAtFixedRate(final Runnable command, long initialDelay, long period, TimeUnit unit);

    void scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit);

    Data toData(Object object);

    Object toObject(Object object);

    TransactionImpl getTransaction();
}
