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

package com.hazelcast.spi;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.storage.DataRef;
import com.hazelcast.storage.Storage;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.wan.WanReplicationService;

/**
 * @author mdogan 8/24/12
 */
public interface NodeEngine {

    OperationService getOperationService();

    ExecutionService getExecutionService();

    ClusterService getClusterService();

    InternalPartitionService getPartitionService();

    EventService getEventService();

    SerializationService getSerializationService();

    ProxyService getProxyService();

    WaitNotifyService getWaitNotifyService();

    WanReplicationService getWanReplicationService();

    TransactionManagerService getTransactionManagerService();

    Address getMasterAddress();

    Address getThisAddress();

    MemberImpl getLocalMember();

    Config getConfig();

    ClassLoader getConfigClassLoader();

    GroupProperties getGroupProperties();

    ILogger getLogger(String name);

    ILogger getLogger(Class clazz);

    Data toData(Object object);

    <T> T toObject(Object object);

    boolean isActive();

    HazelcastInstance getHazelcastInstance();

    <T extends SharedService> T getSharedService(String serviceName);

    Storage<DataRef> getOffHeapStorage();
}
