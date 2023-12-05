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

package com.hazelcast.internal.namespace.iexecutor;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.internal.namespace.UCDTest;

public abstract class IExecutorUCDTest extends UCDTest {
    protected ExecutorConfig executorConfig;
    protected IExecutorService executor;

    @Override
    protected void initialiseConfig() {
        executorConfig = new ExecutorConfig(objectName);
        executorConfig.setNamespace(getNamespaceName());
    }

    @Override
    protected void initialiseDataStructure() {
        executor = instance.getExecutorService(objectName);
    }

    @Override
    protected void registerConfig(Config config) {
        config.addExecutorConfig(executorConfig);
    }
}
