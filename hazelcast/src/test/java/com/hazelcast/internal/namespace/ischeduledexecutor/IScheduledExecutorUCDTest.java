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

package com.hazelcast.internal.namespace.ischeduledexecutor;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.internal.namespace.UCDTest;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;

public abstract class IScheduledExecutorUCDTest extends UCDTest {
    protected ScheduledExecutorConfig scheduledExecutorConfig;
    protected IScheduledExecutorService executor;

    @Override
    protected void initialiseConfig() {
        scheduledExecutorConfig = new ScheduledExecutorConfig(objectName);
        scheduledExecutorConfig.setNamespace(getNamespaceName());
    }

    @Override
    protected void initialiseDataStructure() {
        executor = instance.getScheduledExecutorService(objectName);
    }

    @Override
    protected void registerConfig(Config config) {
        config.addScheduledExecutorConfig(scheduledExecutorConfig);
    }

    protected Member getTargetLocalMember() {
        // We need a `Member` to execute on, which should be from our `instance` driver,
        //  but when that instance is a Client, we need to use a target member
        if (connectionStyle == ConnectionStyle.CLIENT_TO_MEMBER) {
            return member.getCluster().getLocalMember();
        }
        return instance.getCluster().getLocalMember();
    }
}
