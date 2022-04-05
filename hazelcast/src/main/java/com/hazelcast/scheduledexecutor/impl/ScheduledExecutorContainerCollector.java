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

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.merge.AbstractContainerCollector;

import java.util.Collection;
import java.util.Iterator;

class ScheduledExecutorContainerCollector extends AbstractContainerCollector<ScheduledExecutorContainer> {

    private final ScheduledExecutorPartition[] partitions;

    ScheduledExecutorContainerCollector(NodeEngine nodeEngine, ScheduledExecutorPartition[] partitions) {
        super(nodeEngine);
        this.partitions = partitions;
    }

    @Override
    protected Iterator<ScheduledExecutorContainer> containerIterator(int partitionId) {
        ScheduledExecutorPartition partition = partitions[partitionId];
        if (partition == null) {
            return new EmptyIterator();
        }
        return partition.iterator();
    }

    @Override
    public MergePolicyConfig getMergePolicyConfig(ScheduledExecutorContainer container) {
        ScheduledExecutorConfig config = container.getNodeEngine().getConfig()
                .findScheduledExecutorConfig(container.getName());
        return config.getMergePolicyConfig();
    }

    @Override
    protected void destroy(ScheduledExecutorContainer container) {
        container.destroy();
    }

    @Override
    protected void destroyBackup(ScheduledExecutorContainer container) {
        container.destroy();
    }

    @Override
    protected int getMergingValueCount() {
        int size = 0;
        for (Collection<ScheduledExecutorContainer> containers : getCollectedContainers().values()) {
            for (ScheduledExecutorContainer container : containers) {
                size += container.tasks.size();
            }
        }
        return size;
    }
}
