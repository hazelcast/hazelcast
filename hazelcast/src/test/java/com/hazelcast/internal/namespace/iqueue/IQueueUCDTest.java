/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.namespace.iqueue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.internal.namespace.UCDTest;

public abstract class IQueueUCDTest extends UCDTest {
    protected QueueConfig queueConfig;
    protected IQueue<Object> queue;

    @Override
    protected void initialiseConfig() {
        queueConfig = new QueueConfig(objectName);
        queueConfig.setNamespace(getNamespaceName());
    }

    @Override
    protected void initialiseDataStructure() {
        queue = instance.getQueue(objectName);
    }

    @Override
    protected void registerConfig(Config config) {
        config.addQueueConfig(queueConfig);
    }
}
