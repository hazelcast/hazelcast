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

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public abstract class AbstractScheduledExecutorContainerHolder
        implements ScheduledExecutorContainerHolder {

    final NodeEngine nodeEngine;

    /**
     * Containers for scheduled tasks, grouped by scheduler name
     */
    final ConcurrentMap<String, ScheduledExecutorContainer> containers = new ConcurrentHashMap<>();

    public AbstractScheduledExecutorContainerHolder(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public ScheduledExecutorContainer getContainer(String name) {
        checkNotNull(name, "Name can't be null");
        return containers.get(name);
    }

    public ScheduledExecutorContainer getOrCreateContainer(String name) {
        checkNotNull(name, "Name can't be null");

        return getOrPutIfAbsent(containers, name, getContainerConstructorFunction());
    }

    public Collection<ScheduledExecutorContainer> getContainers() {
        return Collections.unmodifiableCollection(containers.values());
    }

    public Iterator<ScheduledExecutorContainer> iterator() {
        return containers.values().iterator();
    }

    public void destroy() {
        for (ScheduledExecutorContainer container : containers.values()) {
            nodeEngine.getExecutionService().shutdownScheduledDurableExecutor(container.getName());
        }
    }

    @Override
    public void destroyContainer(String name) {
        ScheduledExecutorContainer container = containers.remove(name);
        if (container != null) {
            container.destroy();
        }
    }

    protected abstract ConstructorFunction<String, ScheduledExecutorContainer> getContainerConstructorFunction();
}
