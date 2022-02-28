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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.merge.AbstractNamedContainerCollector;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

class QueueContainerCollector extends AbstractNamedContainerCollector<QueueContainer> {

    QueueContainerCollector(NodeEngine nodeEngine, ConcurrentMap<String, QueueContainer> containers) {
        super(nodeEngine, containers);
    }

    @Override
    protected MergePolicyConfig getMergePolicyConfig(QueueContainer container) {
        return container.getConfig().getMergePolicyConfig();
    }

    @Override
    protected void destroy(QueueContainer container) {
        // owned data is stored in the item queue
        container.getItemQueue().clear();
    }

    @Override
    protected void destroyBackup(QueueContainer container) {
        // backup data is stored in the backup map
        container.getBackupMap().clear();
    }

    @Override
    protected int getMergingValueCount() {
        int size = 0;
        for (Collection<QueueContainer> containers : getCollectedContainers().values()) {
            for (QueueContainer container : containers) {
                size += container.size();
            }
        }
        return size;
    }
}
