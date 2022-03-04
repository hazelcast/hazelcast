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

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class TestContainerCollector extends AbstractContainerCollector<Object> {

    final Map<String, Object> containers = new HashMap<String, Object>();

    private final boolean hasContainers;
    private final boolean isMergeable;

    boolean onDestroyHasBeenCalled;

    TestContainerCollector(NodeEngine nodeEngine, boolean hasContainers, boolean isMergeable) {
        super(nodeEngine);
        this.hasContainers = hasContainers;
        this.isMergeable = isMergeable;

        if (hasContainers) {
            containers.put("myContainer", new Object());
        }
    }

    @Override
    protected Iterator<Object> containerIterator(int partitionId) {
        if (hasContainers && partitionId == 0) {
            return containers.values().iterator();
        }
        return new EmptyIterator();
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
    protected void onDestroy() {
        super.onDestroy();
        onDestroyHasBeenCalled = true;
    }

    @Override
    protected int getMergingValueCount() {
        return getCollectedContainers().size();
    }

    @Override
    protected boolean isMergeable(Object container) {
        return isMergeable && super.isMergeable(container);
    }
}
