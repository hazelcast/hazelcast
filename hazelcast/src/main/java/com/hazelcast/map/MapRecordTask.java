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

package com.hazelcast.map;

import com.hazelcast.client.ClientCommandHandler;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapServiceConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Member;
import com.hazelcast.impl.DataAwareEntryEvent;
import com.hazelcast.impl.DefaultRecord;
import com.hazelcast.impl.Record;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.client.MapGetHandler;
import com.hazelcast.map.proxy.DataMapProxy;
import com.hazelcast.map.proxy.MapProxy;
import com.hazelcast.map.proxy.ObjectMapProxy;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.TransactionException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.hazelcast.map.MapService.MAP_SERVICE_NAME;

public class MapRecordTask implements Runnable {

    NodeEngine nodeEngine;
    MapRecordStateOperation operation;
    int partitionId;


    public MapRecordTask(NodeEngine nodeEngine, MapRecordStateOperation operation, int partitionId) {
        this.nodeEngine = nodeEngine;
        this.operation = operation;
        this.partitionId = partitionId;
    }

    public void run() {
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future invoke = invocation.invoke();
            invoke.get();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ExecutionException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

}
