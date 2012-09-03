/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.spi;

import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.impl.TransactionImpl;
import com.hazelcast.impl.map.GenericBackupOperation;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Data;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * @mdogan 8/24/12
 */
public interface NodeService {

    public static final int EXECUTOR_THREAD_ID = -1;
    public static final int EVENT_THREAD_ID = -2;

    void runLocally(Operation op);

    Map<Integer, Object> invokeOnAllPartitions(String serviceName, Operation op) throws Exception;

    SingleInvocationBuilder createSingleInvocation(String serviceName, Operation op, int partitionId);

    boolean send(Operation op, int partitionId, int replicaIndex);

    boolean send(Operation op, int partitionId, Address target);

    boolean send(Operation op, int partitionId, Connection connection);

    void takeBackups(String serviceName, Operation op, int partitionId, int backupCount, int timeoutSeconds)
            throws ExecutionException, TimeoutException, InterruptedException;

    void sendBackups(String serviceName, GenericBackupOperation op, int partitionId, int backupCount);

    void registerService(String serviceName, Object obj);

    <T> T getService(String serviceName);

    Address getThisAddress();

    int getPartitionId(Data key);

    int getPartitionCount();

    PartitionInfo getPartitionInfo(int partitionId);

    Config getConfig();

    GroupProperties getGroupProperties();

    Cluster getCluster();

    ILogger getLogger(String name);

    ExecutorService getExecutorService();

    ScheduledExecutorService getScheduledExecutorService();

    Data toData(Object object);

    Object toObject(Object object);

    TransactionImpl getTransaction();
}
