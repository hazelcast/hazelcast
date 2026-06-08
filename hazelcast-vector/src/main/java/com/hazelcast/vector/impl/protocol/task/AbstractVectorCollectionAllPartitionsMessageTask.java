/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.vector.impl.VectorCollectionService;
import com.hazelcast.vector.impl.stats.LocalVectorCollectionStatsImpl;

import java.util.function.BiConsumer;

import static com.hazelcast.vector.impl.VectorCollectionService.SERVICE_NAME;

public abstract class AbstractVectorCollectionAllPartitionsMessageTask<P> extends AbstractAllPartitionsMessageTask<P> {
    private transient long startTimeNanos;

    public AbstractVectorCollectionAllPartitionsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void beforeProcess() {
        startTimeNanos = Timer.nanos();
    }

    protected void recordStats(BiConsumer<LocalVectorCollectionStatsImpl, Long> statsUpdater) {
        VectorCollectionService service = getService(SERVICE_NAME);
        statsUpdater.accept(service.getStatistics(getDistributedObjectName()),
                Timer.nanosElapsed(startTimeNanos));
    }

    @Override
    public String getServiceName() {
        return VectorCollectionService.SERVICE_NAME;
    }

    @Override
    protected String getUserCodeNamespace() {
        return null;
    }
}
