/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class ScheduledExecutorMemberBin implements ScheduledExecutorContainerHolder {

    private final NodeEngine nodeEngine;

    private final ConcurrentMap<String, ScheduledExecutorContainer> containers =
            new ConcurrentHashMap<String, ScheduledExecutorContainer>();

    private final ConstructorFunction<String, ScheduledExecutorContainer> containerConstructorFunction =
            new ConstructorFunction<String, ScheduledExecutorContainer>() {
                @Override
                public ScheduledExecutorContainer createNew(String name) {
                    ScheduledExecutorConfig config = nodeEngine.getConfig().findScheduledExecutorConfig(name);
                    return new ScheduledExecutorMemberOwnedContainer(name, config.getCapacity(), nodeEngine);
                }
            };

    public ScheduledExecutorMemberBin(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public ScheduledExecutorContainer getOrCreateContainer(String name) {
        checkNotNull(name, "Name can't be null");

        return getOrPutIfAbsent(containers, name, containerConstructorFunction);
    }

    public Collection<ScheduledExecutorContainer> getContainers() {
        return Collections.unmodifiableCollection(containers.values());
    }

    public void destroy() {
        for (ScheduledExecutorContainer container : containers.values()) {
            ((InternalExecutionService) nodeEngine.getExecutionService())
                    .shutdownScheduledDurableExecutor(container.getName());
        }
    }

}
