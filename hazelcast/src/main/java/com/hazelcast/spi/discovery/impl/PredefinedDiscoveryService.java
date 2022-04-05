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

package com.hazelcast.spi.discovery.impl;

import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

import java.util.Map;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Discovery service with a predefined and supplied discovery strategy. All methods delegate to the provided strategy.
 */
public class PredefinedDiscoveryService implements DiscoveryService {
    private final DiscoveryStrategy strategy;

    public PredefinedDiscoveryService(DiscoveryStrategy strategy) {
        this.strategy = checkNotNull(strategy, "Discovery strategy should be not-null");
    }

    @Override
    public void start() {
        strategy.start();
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        return strategy.discoverNodes();
    }

    @Override
    public Map<String, String> discoverLocalMetadata() {
        return strategy.discoverLocalMetadata();
    }

    @Override
    public void destroy() {
        strategy.destroy();
    }
}
