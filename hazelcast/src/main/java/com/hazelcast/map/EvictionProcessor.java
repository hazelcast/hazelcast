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

package com.hazelcast.map;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.operation.EvictOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import static com.hazelcast.map.MapService.SERVICE_NAME;

public class EvictionProcessor implements ScheduledEntryProcessor<Data, Object>{

    final NodeEngine nodeEngine;
    final MapService mapService;
    final String mapName;

    public EvictionProcessor(NodeEngine nodeEngine, MapService mapService, String mapName) {
        this.nodeEngine = nodeEngine;
        this.mapService = mapService;
        this.mapName = mapName;
    }

    public void process(EntryTaskScheduler<Data, Object> scheduler, Collection<ScheduledEntry<Data, Object>> entries) {
        final Collection<Future> futures = new ArrayList<Future>(entries.size());
        final ILogger logger = nodeEngine.getLogger(getClass());

        for (ScheduledEntry<Data, Object> entry : entries) {
            Data key = entry.getKey();
            Operation operation = new EvictOperation(mapName, key, true);
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            try {
                Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .build();
                Future f = invocation.invoke();
                futures.add(f);
            } catch (Throwable t) {
                logger.warning(t);
            }
        }
        for (Future future : futures) {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                logger.finest(e);
            } catch (Exception e) {
                logger.warning(e);
            }
        }
    }

}
