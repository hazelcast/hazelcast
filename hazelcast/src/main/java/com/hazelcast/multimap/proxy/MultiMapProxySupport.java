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

package com.hazelcast.multimap.proxy;

import com.hazelcast.multimap.MultiMapOperation;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.multimap.processor.EntryProcessor;
import com.hazelcast.multimap.processor.GetEntryProcessor;
import com.hazelcast.multimap.processor.PutEntryProcessor;
import com.hazelcast.nio.Data;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.concurrent.Future;

/**
 * @ali 1/1/13
 */
public abstract class MultiMapProxySupport {

    String name;

    MultiMapService service;

    NodeEngine nodeEngine;

    protected MultiMapProxySupport(String name, MultiMapService service, NodeEngine nodeEngine) {
        this.name = name;
        this.service = service;
        this.nodeEngine = nodeEngine;
    }

    Boolean putInternal(Data dataKey, Data dataValue){
        return invoke(dataKey, new PutEntryProcessor(dataValue));
    }

    Collection<Data> getInternal(Data dataKey){
        return invoke(dataKey, new GetEntryProcessor());
    }

    <T> T invoke(Data dataKey, EntryProcessor processor) {
        try {
            int partitionId = nodeEngine.getPartitionId(dataKey);
            MultiMapOperation operation = new MultiMapOperation(name, dataKey, processor, partitionId);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(MultiMapService.MULTI_MAP_SERVICE_NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (T) nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }
}
