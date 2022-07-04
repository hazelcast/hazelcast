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

package com.hazelcast.client.impl.protocol.task.multimap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.monitor.impl.LocalMultiMapStatsImpl;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapService;

public abstract class AbstractMultiMapPartitionMessageTask<P> extends AbstractPartitionMessageTask<P> {


    protected AbstractMultiMapPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    protected void updateStats(ConsumerEx<LocalMultiMapStatsImpl> statsUpdaterFn) {
        if (getContainer().getConfig().isStatisticsEnabled()) {
            statsUpdaterFn.accept(((MultiMapService) getService(MultiMapService.SERVICE_NAME))
                    .getLocalMultiMapStatsImpl(getDistributedObjectName()));
        }
    }

    protected MultiMapContainer getContainer() {
        MultiMapService service = getService(MultiMapService.SERVICE_NAME);
        return service.getOrCreateCollectionContainer(getPartitionId(), getDistributedObjectName());
    }
}
