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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.partition.IPartitionService;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentMap;

/**
 * Base implementation of {@link AbstractContainerCollector}
 * for data structures which reference their containers by name.
 *
 * @param <C> container of the data structure
 */
public abstract class AbstractNamedContainerCollector<C> extends AbstractContainerCollector<C> {

    protected final ConcurrentMap<String, C> containers;

    private final IPartitionService partitionService;

    protected AbstractNamedContainerCollector(NodeEngine nodeEngine, ConcurrentMap<String, C> containers) {
        super(nodeEngine);
        this.containers = containers;
        this.partitionService = nodeEngine.getPartitionService();
    }

    @Override
    protected final Iterator<C> containerIterator(int partitionId) {
        return new ContainerIterator(partitionId);
    }

    /**
     * Will be called by {@link ContainerIterator#hasNext()}.
     * <p>
     * Can be overridden by implementations to save the relation between container name and container.
     *
     * @param containerName the name of the removed container
     * @param container     the removed container
     */
    protected void onIteration(String containerName, C container) {
        // NOP
    }

    int getContainerPartitionId(String containerName) {
        String partitionKey = StringPartitioningStrategy.getPartitionKey(containerName);
        return partitionService.getPartitionId(partitionKey);
    }

    class ContainerIterator implements Iterator<C> {

        private final Iterator<Map.Entry<String, C>> containerEntryIterator;
        private final int partitionId;

        private boolean hasNextWasCalled;
        private Map.Entry<String, C> currentContainerEntry;

        ContainerIterator(int partitionId) {
            this.containerEntryIterator = containers.entrySet().iterator();
            this.partitionId = partitionId;
        }

        @Override
        public boolean hasNext() {
            hasNextWasCalled = true;
            while (containerEntryIterator.hasNext()) {
                Map.Entry<String, C> next = containerEntryIterator.next();
                if (getContainerPartitionId(next.getKey()) == partitionId) {
                    onIteration(next.getKey(), next.getValue());
                    currentContainerEntry = next;
                    return true;
                }
            }
            currentContainerEntry = null;
            return false;
        }

        /**
         * @throws IllegalStateException when called before {@link #hasNext()}
         */
        @Override
        public C next() {
            if (!hasNextWasCalled) {
                throw new IllegalStateException("This iterator needs hasNext() to be called before next()");
            }
            hasNextWasCalled = false;
            if (currentContainerEntry != null) {
                return currentContainerEntry.getValue();
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            if (currentContainerEntry != null) {
                containerEntryIterator.remove();
            }
        }
    }
}
