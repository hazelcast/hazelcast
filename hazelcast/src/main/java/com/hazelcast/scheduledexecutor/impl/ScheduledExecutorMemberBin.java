/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.monitor.LocalScheduledExecutorStats;
import com.hazelcast.monitor.impl.LocalScheduledExecutorStatsImpl;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;

public class ScheduledExecutorMemberBin
        extends AbstractScheduledExecutorContainerHolder {

    private final ILogger logger;

    private final NodeEngine nodeEngine;

    private final ConstructorFunction<String, LocalScheduledExecutorStatsImpl> statsConstructorFunction
            = new ConstructorFunction<String, LocalScheduledExecutorStatsImpl>() {
        public LocalScheduledExecutorStatsImpl createNew(String key) {
            return new LocalScheduledExecutorStatsImpl();
        }
    };

    private final ConstructorFunction<String, ScheduledExecutorContainer> containerConstructorFunction =
            new ConstructorFunction<String, ScheduledExecutorContainer>() {
                @Override
                public ScheduledExecutorContainer createNew(String name) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("[Partition: -1] Create new scheduled executor container with name: " + name);
                    }

                    LocalScheduledExecutorStatsImpl stats = ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, statsConstructorFunction);
                    ScheduledExecutorConfig config = nodeEngine.getConfig().findScheduledExecutorConfig(name);
                    return new ScheduledExecutorMemberOwnedContainer(name, config.getCapacity(), nodeEngine, stats);
                }
            };

    private final ConcurrentHashMap<String, LocalScheduledExecutorStatsImpl> statsMap;

    public ScheduledExecutorMemberBin(NodeEngine nodeEngine, ConcurrentHashMap<String, LocalScheduledExecutorStatsImpl> statsMap) {
        super(nodeEngine);
        this.logger = nodeEngine.getLogger(getClass());
        this.nodeEngine = nodeEngine;
        this.statsMap =statsMap;
    }

    @Override
    public ConstructorFunction<String, ScheduledExecutorContainer> getContainerConstructorFunction() {
        return containerConstructorFunction;
    }

}
