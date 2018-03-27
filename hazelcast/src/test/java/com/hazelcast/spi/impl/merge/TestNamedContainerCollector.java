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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.singletonMap;

class TestNamedContainerCollector extends AbstractNamedContainerCollector<Object> {

    private final boolean isMergeable;

    TestNamedContainerCollector(NodeEngine nodeEngine, boolean hasContainer, boolean isMergeable) {
        super(nodeEngine, hasContainer
                ? new ConcurrentHashMap<String, Object>(singletonMap("myContainer", new Object()))
                : new ConcurrentHashMap<String, Object>());
        this.isMergeable = isMergeable;
    }

    @Override
    protected MergePolicyConfig getMergePolicyConfig(Object container) {
        return new MergePolicyConfig();
    }

    @Override
    protected void destroy(Object container) {
    }

    @Override
    protected void destroyBackup(Object container) {
    }

    @Override
    protected boolean isMergeable(Object container) {
        return isMergeable && super.isMergeable(container);
    }

    @Override
    protected int getMergingValueCount() {
        return getCollectedContainers().size();
    }
}
