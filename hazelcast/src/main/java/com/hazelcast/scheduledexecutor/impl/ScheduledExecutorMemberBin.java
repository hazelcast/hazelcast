/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConstructorFunction;

public class ScheduledExecutorMemberBin
        extends AbstractScheduledExecutorContainerHolder {

    private final ILogger logger;

    private final NodeEngine nodeEngine;

    private final ConstructorFunction<String, ScheduledExecutorContainer> containerConstructorFunction =
            new ConstructorFunction<String, ScheduledExecutorContainer>() {
                @Override
                public ScheduledExecutorContainer createNew(String name) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("[Partition: -1] Create new scheduled executor container with name: " + name);
                    }

                    ScheduledExecutorConfig config = nodeEngine.getConfig().findScheduledExecutorConfig(name);
                    return new ScheduledExecutorMemberOwnedContainer(name, config.getCapacity(), nodeEngine);
                }
            };

    public ScheduledExecutorMemberBin(NodeEngine nodeEngine) {
        super(nodeEngine);
        this.logger = nodeEngine.getLogger(getClass());
        this.nodeEngine = nodeEngine;
    }

    @Override
    public ConstructorFunction<String, ScheduledExecutorContainer> getContainerConstructorFunction() {
        return containerConstructorFunction;
    }

}
